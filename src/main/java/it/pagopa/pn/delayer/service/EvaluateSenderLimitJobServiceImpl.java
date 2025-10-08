package it.pagopa.pn.delayer.service;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
import it.pagopa.pn.delayer.utils.DeliveryDriverUtils;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import it.pagopa.pn.delayer.utils.PnDelayerUtils;
import it.pagopa.pn.delayer.utils.SenderLimitUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.PAPER_DELIVERY_PRIORITY_MAP_NOT_FOUND;

@Component
@Slf4j
@RequiredArgsConstructor
public class EvaluateSenderLimitJobServiceImpl implements EvaluateSenderLimitJobService {

    private final PnDelayerUtils pnDelayerUtils;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final PaperDeliveryUtils paperDeliveryUtils;
    private final DeliveryDriverUtils deliveryDriverUtils;
    private final SsmParameterConsumerActivation ssmParameterConsumerActivation;
    private final SenderLimitUtils senderLimitUtils;
    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;


    @Override
    public Mono<Void> startSenderLimitJob(String province, String tenderId, Instant startExecutionBatch) {
        LocalDate deliveryWeek = pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch);
        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();
        senderLimitJobProcessObjects.setPriorityMap(getPriorityMap());
        return senderLimitUtils.retrieveTotalEstimateCounter(deliveryWeek, province)
                .doOnNext(senderLimitJobProcessObjects::setTotalEstimateCounter)
                .flatMap(stringIntegerMap -> deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .flatMap(driversTotalCapacities -> retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, new HashMap<>(), driversTotalCapacities, senderLimitJobProcessObjects))
                .flatMap(incrementUsedSenderLimitDtoList -> flushCounters(deliveryWeek, incrementUsedSenderLimitDtoList))
                .doOnError(error -> log.error("Error processing sender limit job for province: {}, tenderId: {}, deliveryWeek: {}", province, tenderId, deliveryWeek, error));
    }

    private Mono<List<IncrementUsedSenderLimitDto>> retrieveAndProcessPaperDeliveries(String province, String tenderId, LocalDate deliveryWeek, Map<String, AttributeValue> lastEvaluatedKey, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        var sortkeyPrefix = province + "~";
        return paperDeliveryUtils.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, deliveryWeek, sortkeyPrefix, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit())
                .flatMap(paperDeliveryPage -> processItems(paperDeliveryPage.items(), tenderId, deliveryWeek, driversTotalCapacity, senderLimitJobProcessObjects)
                        .flatMap(processObjects -> {
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey())) {
                                log.info("Processed items for province: {}, tenderId: {}, deliveryWeek: {}. Continuing with lastEvaluatedKey: {}", province, tenderId, deliveryWeek, paperDeliveryPage.lastEvaluatedKey());
                                processObjects.getSendToDriverCapacityStep().clear();
                                processObjects.getSendToResidualCapacityStep().clear();
                                return retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, paperDeliveryPage.lastEvaluatedKey(), driversTotalCapacity, processObjects);
                            }
                            log.info("Processed items for province: {}, tenderId: {}, deliveryWeek: {}. No more items to process.", province, tenderId, deliveryWeek);
                            return Mono.just(processObjects.getIncrementUsedSenderLimitDtoList());
                        }));
    }

    private Mono<Void> flushCounters(LocalDate deliveryDate, List<IncrementUsedSenderLimitDto> incrementUsedSenderLimitList) {
        LocalDate shipmentDate = deliveryDate.minusWeeks(1);

        if (CollectionUtils.isEmpty(incrementUsedSenderLimitList)) {
            return Mono.empty();
        }

        Map<String, Long> totalsByGeo = incrementUsedSenderLimitList.stream()
                .collect(Collectors.groupingBy(
                        incrementUsedSenderLimitDto -> String.join("#", incrementUsedSenderLimitDto.pk(),
                                incrementUsedSenderLimitDto.senderLimit().toString()),
                        Collectors.summingLong(dto -> dto.increment() == null ? 0 : dto.increment())
                ));


        log.info("Total used capacities to update: {}", totalsByGeo.size());

        return Flux.fromIterable(totalsByGeo.entrySet())
                .flatMap(incrementUsedCapacityEntry ->
                        paperDeliverySenderLimitDAO.updateUsedSenderLimit(
                                incrementUsedCapacityEntry.getKey().split("#")[0],
                                incrementUsedCapacityEntry.getValue(),
                                shipmentDate,
                                Integer.valueOf(incrementUsedCapacityEntry.getKey().split("#")[1]))
                )
                .then();
    }

    /**
     * Processes the list of paper deliveries through the following steps:
     * 1. Retrieves the unified delivery drivers and assigns them to each paper delivery.
     * 2. Excludes deliveries marked as RS or Second Attempt from sender limit evaluation,
     * and includes them directly in the list for the EVALUATE_DRIVER_CAPACITY step.
     * 3. Groups the remaining deliveries by product type, PaId, and province.
     * 4. Calculates and evaluates the sender limit for each group,returning a SenderLimitJobPaperDeliveries object
     * that categorizes deliveries for either the EVALUATE_DRIVER_CAPACITY or EVALUATE_RESIDUAL_CAPACITY step.
     * 5. Inserts new entities into the Pn-DelayerPaperDeliveries collection, mapped to their corresponding evaluation step.
     * 6. Updates the used sender limits in the database for deliveries assigned to the EVALUATE_DRIVER_CAPACITY step.
     *
     * @param items                List of PaperDelivery items to be processed
     * @param tenderId             The tender ID associated with the deliveries
     * @param deliveryWeek         The week of delivery for which the items are being processed
     * @param driversTotalCapacity List of DriversTotalCapacity containing unified delivery drivers and their capacities
     * @return Mono<Long> indicating the count of items sent to the next step
     */
    private Mono<SenderLimitJobProcessObjects> processItems(List<PaperDelivery> items, String tenderId, LocalDate deliveryWeek, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        return retrieveUnifiedDeliveryDriverAndAssignToPaperDeliveries(items, tenderId, driversTotalCapacity, senderLimitJobProcessObjects.getPriorityMap())
                .map(paperDeliveryList -> pnDelayerUtils.excludeRsAndSecondAttempt(paperDeliveryList, senderLimitJobProcessObjects))
                .map(pnDelayerUtils::groupByPaIdProductTypeProvince)
                .flatMap(deliveriesGroupedByProductTypePaId -> senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, driversTotalCapacity, senderLimitJobProcessObjects))
                .flatMap(deliveries -> paperDeliveryUtils.insertPaperDeliveries(deliveries, deliveryWeek))
                .filter(sentToNextStep -> !CollectionUtils.isEmpty(sentToNextStep))
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No items to send to evaluate driver capacity step for tenderId: {}, deliveryWeek: {}", tenderId, deliveryWeek);
                    return Mono.empty();
                }))
                .flatMapMany(sentToNextStep -> senderLimitUtils.createIncrementUsedSenderLimitDtos(sentToNextStep, senderLimitJobProcessObjects.getSenderLimitMap()))
                .collectList()
                .doOnNext(incrementUsedSenderLimitDtoList -> senderLimitJobProcessObjects.getIncrementUsedSenderLimitDtoList().addAll(incrementUsedSenderLimitDtoList))
                .thenReturn(senderLimitJobProcessObjects);
    }

    /**
     * This method checks whether there is only one unified delivery driver available for the given province.
     * If exactly one driver is found, it assigns that driver to all paper deliveries.
     * Otherwise, it attempts to retrieve the list of unified delivery drivers from the internal cache.
     * If the drivers are not found in the cache, it retrieves them from the Paper Channel Lambda,
     * assigns them to the paper deliveries, and updates the cache accordingly.
     */
    private Mono<List<PaperDelivery>> retrieveUnifiedDeliveryDriverAndAssignToPaperDeliveries(List<PaperDelivery> paperDelivery, String tenderId, List<DriversTotalCapacity> driversTotalCapacity, Map<Integer, List<String>> priorityMap) {
        if (driversTotalCapacity.size() == 1 && driversTotalCapacity.getFirst().getUnifiedDeliveryDrivers().size() == 1) {
            String unifiedDeliveryDriver = driversTotalCapacity.getFirst().getUnifiedDeliveryDrivers().getFirst();
            return Mono.just(deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(paperDelivery, unifiedDeliveryDriver, tenderId, priorityMap));
        } else {
            Map<String, List<PaperDelivery>> groupedByCapProductType = pnDelayerUtils.groupByCapAndProductType(paperDelivery);
            log.info("Number of CAP and Product Type groups to process for tenderId {}: {}", tenderId, groupedByCapProductType.size());
            ConcurrentHashMap<String, List<PaperDelivery>> groupedByCapProductTypeNotInCache = new ConcurrentHashMap<>();
            return Flux.fromIterable(groupedByCapProductType.entrySet())
                    .map(capProductTypeEntry -> retrieveFromCacheAndEnrichPaperDelivery(tenderId, priorityMap, capProductTypeEntry, groupedByCapProductTypeNotInCache))
                    .flatMapIterable(list -> list)
                    .collectList()
                    .flatMap(toSenderLimitEvaluationTmp -> callPaperChannelAndRetrieveEnrichedPaperDelivery(tenderId, priorityMap, groupedByCapProductTypeNotInCache)
                            .map(toSenderLimitEvaluationFromPaperChannel -> {
                                List<PaperDelivery> result = new ArrayList<>(toSenderLimitEvaluationTmp);
                                result.addAll(toSenderLimitEvaluationFromPaperChannel);
                                return result;
                            }));
        }
    }

    private List<PaperDelivery> retrieveFromCacheAndEnrichPaperDelivery(String tenderId, Map<Integer, List<String>> priorityMap, Map.Entry<String, List<PaperDelivery>> capProductTypeEntry, Map<String, List<PaperDelivery>> groupedByCapProductTypeNotInCache) {
        return deliveryDriverUtils.retrieveFromCache(capProductTypeEntry.getKey())
                .map(unifiedDeliveryDriver -> deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(capProductTypeEntry.getValue(), unifiedDeliveryDriver, tenderId, priorityMap))
                .orElseGet(() -> {
                    groupedByCapProductTypeNotInCache.put(capProductTypeEntry.getKey(), capProductTypeEntry.getValue());
                    return new ArrayList<>();
                });
    }

    private Mono<List<PaperDelivery>> callPaperChannelAndRetrieveEnrichedPaperDelivery(String tenderId, Map<Integer, List<String>> priorityMap, Map<String, List<PaperDelivery>> groupedByCapProductTypeNotInCache) {

        return Flux.fromIterable(groupedByCapProductTypeNotInCache.keySet())
                .map(capProductTypeKey -> new DeliveryDriverRequest(capProductTypeKey.split("~")[0], capProductTypeKey.split("~")[1]))
                .collectList()
                .doOnNext(requests -> log.info("Number of driver requests for paper channel for tenderId {}: {}", tenderId, requests.size()))
                .filter(requests -> !CollectionUtils.isEmpty(requests))
                .map(requests -> deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(requests, tenderId))
                .doOnNext(deliveryDriverUtils::insertInCache)
                .map(responses -> deliveryDriverUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(groupedByCapProductTypeNotInCache, tenderId, priorityMap))
                .defaultIfEmpty(List.of());
    }

    private Map<Integer, List<String>> getPriorityMap() {
        return ((Map<String, List<String>>) ssmParameterConsumerActivation.getParameterValue(pnDelayerConfigs.getPaperDeliveryPriorityParameterName(), Map.class)
                .orElseThrow(() -> new PnInternalException("Failed to retrieve paper delivery priority map from SSM parameter store", PAPER_DELIVERY_PRIORITY_MAP_NOT_FOUND)))
                .entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.parseInt(e.getKey()), Map.Entry::getValue));
    }
}

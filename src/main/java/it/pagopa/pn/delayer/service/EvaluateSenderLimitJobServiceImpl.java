package it.pagopa.pn.delayer.service;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
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
import reactor.util.function.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

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


    @Override
    public Mono<Void> startSenderLimitJob(String province, String tenderId, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch) {
        LocalDate deliveryWeek = pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch);
        Map<Integer, List<String>> priorityMap = getPriorityMap();
        return deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province)
                .flatMap(driversTotalCapacities -> retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, lastEvaluatedKey, driversTotalCapacities, priorityMap))
                .doOnError(error -> log.error("Error processing sender limit job for province: {}, tenderId: {}, deliveryWeek: {}", province, tenderId, deliveryWeek, error));
    }

    private Mono<Void> retrieveAndProcessPaperDeliveries(String province, String tenderId, LocalDate deliveryWeek, Map<String, AttributeValue> lastEvaluatedKey, List<DriversTotalCapacity> driversTotalCapacity, Map<Integer, List<String>> priorityMap) {
        return paperDeliveryUtils.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, deliveryWeek, province, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit())
                .flatMap(paperDeliveryPage -> processItems(paperDeliveryPage.items(), tenderId, deliveryWeek, driversTotalCapacity, priorityMap)
                        .flatMap(sentToNextStepItemsCount -> {
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey())) {
                                return retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, paperDeliveryPage.lastEvaluatedKey(), driversTotalCapacity, priorityMap);
                            }
                            return Mono.empty();
                        }))
                .then();
    }

    private Mono<Long> processItems(List<PaperDelivery> items, String tenderId, LocalDate deliveryWeek, List<DriversTotalCapacity> driversTotalCapacity, Map<Integer, List<String>> priorityMap) {
        SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries = new SenderLimitJobPaperDeliveries();
        Map<String, Tuple2<Integer, Integer>> senderLimitMap = new HashMap<>();
        return retrieveUnifiedDeliveryDriverAndAssignToPaperDeliveries(items, tenderId, driversTotalCapacity, priorityMap)
                .map(paperDeliveryList -> pnDelayerUtils.excludeRsAndSecondAttempt(items, senderLimitJobPaperDeliveries))
                .map(pnDelayerUtils::groupByPaIdProductTypeProvince)
                .flatMap(deliveriesGroupedByProductTypePaId -> senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, senderLimitMap, driversTotalCapacity, senderLimitJobPaperDeliveries))
                .flatMap(deliveries -> paperDeliveryUtils.insertPaperDeliveries(deliveries, deliveryWeek))
                .flatMap(paperDeliveryList -> senderLimitUtils.updateUsedSenderLimit(paperDeliveryList, deliveryWeek, senderLimitMap));
    }

    private Mono<List<PaperDelivery>> retrieveUnifiedDeliveryDriverAndAssignToPaperDeliveries(List<PaperDelivery> paperDelivery, String tenderId, List<DriversTotalCapacity> driversTotalCapacity, Map<Integer, List<String>> priorityMap) {
        List<PaperDelivery> toSenderLimitEvaluation = new ArrayList<>();
        if (driversTotalCapacity.size() == 1 && driversTotalCapacity.getFirst().getUnifiedDeliveryDrivers().size() == 1){
            String unifiedDeliveryDriver = driversTotalCapacity.getFirst().getUnifiedDeliveryDrivers().getFirst();
            return Mono.just(pnDelayerUtils.enrichWithPriorityAndUnifiedDeliveryDriver(paperDelivery, unifiedDeliveryDriver, tenderId, priorityMap));
        } else {
            Map<String, List<PaperDelivery>> groupedByCapProductType = pnDelayerUtils.groupByCapAndProductType(paperDelivery);
            List<DeliveryDriverRequest> deliveryDriverRequest = new ArrayList<>();
            return Flux.fromIterable(groupedByCapProductType.entrySet())
                    .doOnNext(capProductTypeEntry -> deliveryDriverUtils.retrieveFromCache(capProductTypeEntry.getKey())
                            .ifPresentOrElse(unifiedDeliveryDriver -> toSenderLimitEvaluation.addAll(pnDelayerUtils.enrichWithPriorityAndUnifiedDeliveryDriver(capProductTypeEntry.getValue(), unifiedDeliveryDriver, tenderId, priorityMap)),
                                    () -> deliveryDriverRequest.add(new DeliveryDriverRequest(capProductTypeEntry.getKey().split("~")[0], capProductTypeEntry.getKey().split("~")[1]))))
                    .then(Mono.just(deliveryDriverRequest))
                    .filter(deliveryDriverRequests -> !CollectionUtils.isEmpty(deliveryDriverRequests))
                    .map(deliveryDriverRequests -> deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(deliveryDriverRequests, tenderId))
                    .doOnNext(deliveryDriverUtils::insertInCache)
                    .map(paperChannelDeliveryDriverResponses -> pnDelayerUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(paperChannelDeliveryDriverResponses, groupedByCapProductType, tenderId, priorityMap))
                    .doOnNext(toSenderLimitEvaluation::addAll)
                    .thenReturn(toSenderLimitEvaluation);
        }
    }

    private Map<Integer, List<String>> getPriorityMap() {
        return ssmParameterConsumerActivation.getParameterValue(pnDelayerConfigs.getPaperDeliveryPriorityParameterName(), Map.class)
                .orElseThrow(() -> new PnInternalException("Failed to retrieve paper delivery priority map from SSM parameter store", "PAPER_DELIVERY_PRIORITY_MAP_NOT_FOUND"));
    }
}

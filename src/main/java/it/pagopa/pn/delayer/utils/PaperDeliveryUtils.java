package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaperDeliveryUtils {

    private final PaperDeliveryDAO paperDeliveryDAO;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final PnDelayerUtils pnDelayerUtils;
    private final DeliveryDriverUtils deliveryDriverUtils;
    private final PaperDeliveryCounterDAO paperDeliveryCounterDAO;


    /**
     * Starts the job to evaluate driver capacity for paper deliveries, This method evaluates residual province capacity.
     * If the residual capacity is greater than zero, it retrieves paper deliveries and processes them.
     * If the residual capacity is zero or less, it sends the deliveries to the next week.
     *
     * @param workflowStepEnum      the workflow step enum indicating the current processing step (EVALUATE_DRIVER_CAPACITY, EVALUATE_RESIDUAL_CAPACITY)
     * @param unifiedDeliveryDriver the unified delivery driver identifier
     * @param province              the province for which the capacity is evaluated
     * @param deliveryWeek          the initial day of delivery week (e.g., Monday of current week)
     * @param tenderId              the tender identifier
     */
    public Mono<Void> evaluateCapacitiesAndProcessDeliveries(WorkflowStepEnum workflowStepEnum, String unifiedDeliveryDriver, String province, LocalDate deliveryWeek, String tenderId) {
        return deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(province, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for province: [{}], unifiedDeliveryDriver: [{}] -> declared capacity={}, used capacity={}", province, unifiedDeliveryDriver, tuple.getT1(), tuple.getT2()))
                .flatMap(tuple -> {
                    String sortKeyPrefix = String.join("~", unifiedDeliveryDriver, province);
                    int residualCapacity = tuple.getT1() - tuple.getT2();
                    if (residualCapacity <= 0) {
                        log.warn("No capacity for province={} and unifiedDeliveryDriver={}, no records will be processed", province, unifiedDeliveryDriver);
                        return sendToNextWeek(workflowStepEnum, sortKeyPrefix, new HashMap<>(), deliveryWeek);
                    } else {
                        AtomicInteger printCounter = new AtomicInteger(0);
                        DriverCapacityJobProcessResult driverCapacityJobProcessResult = new DriverCapacityJobProcessResult();
                        return Mono.just(pnDelayerUtils.retrieveActualPrintCapacity(deliveryWeek))
                                .flatMap(dailyPrintCapacity -> sendToNextStep(workflowStepEnum, sortKeyPrefix, new HashMap<>(), tenderId, deliveryWeek, residualCapacity, tuple.getT1(), dailyPrintCapacity * pnDelayerConfigs.getPrintCapacityWeeklyWorkingDays(), printCounter, driverCapacityJobProcessResult));
                    }
                })
                .then();
    }

    private Mono<Void> sendToNextWeek(WorkflowStepEnum workflowStepEnum, String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, LocalDate deliveryWeek) {
        return retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit())
                .flatMap(paperDeliveryPage -> processChunkToSendToNextWeek(paperDeliveryPage.items(), deliveryWeek)
                        .flatMap(unused -> !CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey()) ?
                                sendToNextWeek(workflowStepEnum, sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), deliveryWeek) : Mono.empty()));
    }


    /**
     * Processes a chunk of paper deliveries to send them to the next week because no driver capacity Found for them in current week.
     *
     * @param chunk        the list of paper deliveries to process
     * @param deliveryWeek the delivery week for which the deliveries are processed
     * @return a Mono containing the number of processed deliveries
     */
    private Mono<Integer> processChunkToSendToNextWeek(List<PaperDelivery> chunk, LocalDate deliveryWeek) {
        log.info("Processing chunk of size {} to send to next week", chunk.size());
        if (!CollectionUtils.isEmpty(chunk)) {
            return paperDeliveryDAO.insertPaperDeliveries(pnDelayerUtils.mapItemForEvaluateSenderLimitOnNextWeek(chunk, deliveryWeek))
                    .thenReturn(chunk.size());
        }
        return Mono.just(0);
    }

    /**
     * Sends paper deliveries to the next step of processing, evaluating their capacity and updating counters.
     * If there are more deliveries than the residual capacity, it send the excess deliveries to the next week.
     *
     * @param sortKeyPrefix    the prefix for the sort key used in DynamoDB
     * @param lastEvaluatedKey the last evaluated key for pagination
     * @param tenderId         the tender identifier
     * @param deliveryWeek     the week for which the deliveries are processed
     * @param residualCapacity the remaining capacity for processing deliveries
     * @param declaredCapacity the declared capacity for the province
     * @return a Mono containing the number of deliveries sent to the next step
     */
    private Mono<Integer> sendToNextStep(WorkflowStepEnum workflowStepEnum,
                                         String sortKeyPrefix,
                                         Map<String, AttributeValue> lastEvaluatedKey,
                                         String tenderId,
                                         LocalDate deliveryWeek,
                                         Integer residualCapacity,
                                         Integer declaredCapacity,
                                         Integer weeklyPrintCapacity,
                                         AtomicInteger printCounter,
                                         DriverCapacityJobProcessResult driverCapacityJobProcessResult) {

        String[] splittedSortKeyPrefix = sortKeyPrefix.split("~");
        String unifiedDeliveryDriver = splittedSortKeyPrefix[0];
        String province = splittedSortKeyPrefix[1];

        return retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, Math.min(residualCapacity, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()))
                .flatMap(paperDeliveryPage -> processChunkToSendToNextStep(province, paperDeliveryPage.items(), unifiedDeliveryDriver, tenderId, deliveryWeek, printCounter, driverCapacityJobProcessResult)
                        .flatMap(processResult -> {
                            log.info("driverCapacityJobProcessResult for province={} and unifiedDeliveryDriver={} after processing chunk: sentToNextStep={}, totalIncrements={}",
                                    province, unifiedDeliveryDriver, processResult.getSentToNextStep(), processResult.getIncrementUsedCapacityDtos().size());
                            int residualCapacityAfterSending = residualCapacity - processResult.getSentToNextStep();
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey()) && residualCapacityAfterSending > 0) {
                                log.info("Continuing to process chunk to send to next step for province={} and unifiedDeliveryDriver={}, residualCapacityAfterSending={}", province, unifiedDeliveryDriver, residualCapacityAfterSending);
                                return sendToNextStep(workflowStepEnum, sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), tenderId, deliveryWeek, residualCapacityAfterSending, declaredCapacity, weeklyPrintCapacity, printCounter, processResult)
                                        .thenReturn(residualCapacityAfterSending);
                            } else {
                                log.info("Finished processing chunk to send to next step for province={} and unifiedDeliveryDriver={}, residualCapacityAfterSending={}", province, unifiedDeliveryDriver, residualCapacityAfterSending);
                                processResult.getIncrementUsedCapacityDtos().add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, province, processResult.getSentToNextStep(), deliveryWeek, declaredCapacity));
                                return flushCounters(deliveryWeek, weeklyPrintCapacity, printCounter, driverCapacityJobProcessResult.getIncrementUsedCapacityDtos())
                                        .thenReturn(residualCapacityAfterSending);
                            }
                        })
                        .filter(residualCapacityAfterSending -> residualCapacityAfterSending <= 0 && !CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey()))
                        .doOnNext(integer -> log.info("Process next chunk to send to next week for province={} and unifiedDeliveryDriver={}, residualCapacityAfterSending={}", province, unifiedDeliveryDriver, residualCapacity - driverCapacityJobProcessResult.getSentToNextStep()))
                        .flatMap(unused -> sendToNextWeek(workflowStepEnum, sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), deliveryWeek))
                        .thenReturn(paperDeliveryPage.items().size()));
    }

    private Mono<Void> flushCounters(LocalDate deliveryWeek,
                                     Integer weeklyPrintCapacity,
                                     AtomicInteger printCounter,
                                     List<IncrementUsedCapacityDto> incrementUsedCapacities) {
        if (printCounter.get() == 0 && (incrementUsedCapacities == null || incrementUsedCapacities.isEmpty())) {
            return Mono.empty();
        }

        return paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryWeek, printCounter.get(), weeklyPrintCapacity)
                .then(deliveryDriverUtils.updateCounters(incrementUsedCapacities));
    }


    private Mono<DriverCapacityJobProcessResult> processChunkToSendToNextStep(String province, List<PaperDelivery> chunk, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, AtomicInteger printCounter, DriverCapacityJobProcessResult driverCapacityJobProcessResult) {

        IncrementUsedCapacityDto alreadyUsedCapacity = driverCapacityJobProcessResult.getIncrementUsedCapacityDtos().stream()
                .filter(incrementUsedCapacityDto -> incrementUsedCapacityDto.unifiedDeliveryDriver().equalsIgnoreCase(unifiedDeliveryDriver)
                        && incrementUsedCapacityDto.geoKey().equalsIgnoreCase(province))
                .findFirst()
                .orElse(null);

        return evaluateCapCapacity(chunk, unifiedDeliveryDriver, tenderId, deliveryWeek, alreadyUsedCapacity)
                .collectList()
                .flatMap(driverCapacityObjects -> {
                    // merge toNextStep
                    List<PaperDelivery> deliveriesToSend = driverCapacityObjects.stream()
                            .flatMap(driverCapacityJobProcessObject -> driverCapacityJobProcessObject.getToNextStep().stream())
                            .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));

                    // merge incrementUsedCapacityDtosForCap
                    List<IncrementUsedCapacityDto> mergedIncrements = driverCapacityObjects.stream()
                            .flatMap(driverCapacityJobProcessObject -> driverCapacityJobProcessObject.getIncrementUsedCapacityDtosForCap().stream())
                            .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));

                    // aggiorno il result cumulativo
                    driverCapacityJobProcessResult.setSentToNextStep(driverCapacityJobProcessResult.getSentToNextStep() + deliveriesToSend.size());
                    driverCapacityJobProcessResult.getIncrementUsedCapacityDtos().addAll(mergedIncrements);

                    return paperDeliveryDAO.insertPaperDeliveries(deliveriesToSend)
                            .thenReturn(driverCapacityJobProcessResult)
                            .doOnNext(driverCapacityJobProcessRes -> printCounter.addAndGet(deliveriesToSend.size()));
                });
    }

    public Mono<Page<PaperDelivery>> retrievePaperDeliveries(WorkflowStepEnum workflowStepEnum, LocalDate deliveryWeek, String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, Integer queryLimit) {
        return paperDeliveryDAO.retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, queryLimit)
                .flatMap(paperDeliveryPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryPage.items())) {
                        log.warn("No records found for province = [{}] and deliveryWeek = [{}]", sortKeyPrefix, deliveryWeek);
                        return Mono.empty();
                    }
                    return Mono.just(paperDeliveryPage);
                });
    }

    private Flux<DriverCapacityJobProcessObject> evaluateCapCapacity(List<PaperDelivery> paperDeliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, IncrementUsedCapacityDto incrementUsedCapacityDto) {
        Map<String, List<PaperDelivery>> capMap = pnDelayerUtils.groupByCap(paperDeliveries);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(entry -> processCapGroup(entry.getKey(), entry.getValue(), unifiedDeliveryDriver, tenderId, deliveryWeek, incrementUsedCapacityDto)
                        .doOnNext(driverCapacityJobProcessObject -> log.info("Processed CAP group [{}~{}]: toNextStep={}, toNextWeek={}, increments={}",
                                unifiedDeliveryDriver, entry.getKey(),
                                driverCapacityJobProcessObject.getToNextStep().size(),
                                driverCapacityJobProcessObject.getToNextWeek().size(),
                                driverCapacityJobProcessObject.getIncrementUsedCapacityDtosForCap().size())))
                .flatMap(driverCapacityJobProcessObject -> processChunkToSendToNextWeek(driverCapacityJobProcessObject.getToNextWeek(), deliveryWeek)
                        .thenReturn(driverCapacityJobProcessObject));
    }

    /**
     * Processes a group of paper deliveries based on their CAP (postal code) and evaluates the available capacity.
     * If there is enough capacity, it filters and prepares the deliveries for sending.
     * If there is not enough capacity, it sends the excess deliveries to the next week.
     *
     * @param cap                       the CAP (postal code) for which the deliveries are processed
     * @param deliveries                the list of paper deliveries for the given CAP
     * @param unifiedDeliveryDriver     the unified delivery driver identifier
     * @param tenderId                  the tender identifier
     * @param deliveryWeek              the week for which the deliveries are processed
     * @return a Mono containing the number of processed deliveries
     */
    private Mono<DriverCapacityJobProcessObject> processCapGroup(String cap, List<PaperDelivery> deliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, IncrementUsedCapacityDto incrementUsedCapacityDto) {
        DriverCapacityJobProcessObject driverCapacityJobProcessObject = new DriverCapacityJobProcessObject();
        return evaluateIncrementUsedCapacityDto(incrementUsedCapacityDto)
                .switchIfEmpty(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(cap, unifiedDeliveryDriver, tenderId, deliveryWeek))
                .doOnNext(tuple -> log.info("Retrieved capacities for [{}~{}] -> availableCapacity={}, usedCapacity={}", unifiedDeliveryDriver, cap, tuple.getT1(), tuple.getT2()))
                .flatMap(capCapacityAndUsedCapacity -> Mono.just(pnDelayerUtils.filterOnResidualDriverCapacity(deliveries, capCapacityAndUsedCapacity, driverCapacityJobProcessObject.getToNextStep(), driverCapacityJobProcessObject.getToNextWeek(), deliveryWeek))
                        .filter(deliveriesCount -> deliveriesCount > 0)
                        .doOnDiscard(Integer.class, unused -> log.warn("No capacity for cap={} and unifiedDeliveryDriver={}, no records will be processed", cap, unifiedDeliveryDriver))
                        .doOnNext(deliveriesCount -> driverCapacityJobProcessObject.getIncrementUsedCapacityDtosForCap().add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, cap, deliveriesCount, deliveryWeek, capCapacityAndUsedCapacity.getT1()))))
                .thenReturn(driverCapacityJobProcessObject);
    }

    private Mono<Tuple2<Integer, Integer>> evaluateIncrementUsedCapacityDto(IncrementUsedCapacityDto incrementUsedCapacityDto) {
        if(Objects.nonNull(incrementUsedCapacityDto)){
            return Mono.just(Tuples.of(incrementUsedCapacityDto.declaredCapacity(), incrementUsedCapacityDto.numberOfDeliveries()));
        }
        return Mono.empty();
    }

    public Mono<List<PaperDelivery>> insertPaperDeliveries(SenderLimitJobProcessObjects senderLimitJobProcessObjects, LocalDate deliveryWeek) {
        return paperDeliveryDAO.insertPaperDeliveries(pnDelayerUtils.mapItemForEvaluateDriverCapacityStep(senderLimitJobProcessObjects.getSendToDriverCapacityStep(), deliveryWeek))
                .thenReturn(senderLimitJobProcessObjects)
                .flatMap(unused -> paperDeliveryDAO.insertPaperDeliveries(pnDelayerUtils.mapItemForResidualCapacityStep(senderLimitJobProcessObjects.getSendToResidualCapacityStep(), deliveryWeek))
                        .thenReturn(senderLimitJobProcessObjects.getSendToDriverCapacityStep())
                        .doOnNext(paperDeliveries -> paperDeliveries.removeIf(paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1)));
    }
}

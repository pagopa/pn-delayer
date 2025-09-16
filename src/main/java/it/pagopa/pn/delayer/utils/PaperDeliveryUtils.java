package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import it.pagopa.pn.delayer.model.SenderLimitJobProcessObjects;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                        List<IncrementUsedCapacityDto> incrementCapacities = new ArrayList<>();
                        return Mono.just(pnDelayerUtils.retrieveActualPrintCapacity(deliveryWeek))
                                .flatMap(dailyPrintCapacity -> sendToNextStep(workflowStepEnum, sortKeyPrefix, new HashMap<>(), tenderId, deliveryWeek, residualCapacity, tuple.getT1(), dailyPrintCapacity * pnDelayerConfigs.getPrintCapacityWeeklyWorkingDays(), printCounter, incrementCapacities));
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
    private Mono<Integer> sendToNextStep(WorkflowStepEnum workflowStepEnum, String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, String tenderId, LocalDate deliveryWeek, Integer residualCapacity, Integer declaredCapacity, Integer weeklyPrintCapacity, AtomicInteger printCounter, List<IncrementUsedCapacityDto> incrementUsedCapacities) {
        return retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, Math.min(residualCapacity, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()))
                .flatMap(paperDeliveryPage -> processChunkToSendToNextStep(paperDeliveryPage.items(), sortKeyPrefix, tenderId, deliveryWeek, declaredCapacity, printCounter, incrementUsedCapacities)
                        .flatMap(sentToNextStep -> {
                            int residualCapacityAfterSending = residualCapacity - sentToNextStep;
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey())) {
                                return residualCapacityAfterSending <= 0 ?
                                        sendToNextWeek(workflowStepEnum, sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), deliveryWeek).thenReturn(sentToNextStep) :
                                        sendToNextStep(workflowStepEnum, sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), tenderId, deliveryWeek, residualCapacity - sentToNextStep, declaredCapacity, weeklyPrintCapacity, printCounter, incrementUsedCapacities);
                            }
                            return paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryWeek, printCounter.get(), weeklyPrintCapacity)
                                    .then(deliveryDriverUtils.updateCounters(incrementUsedCapacities))
                                    .then(Mono.empty());
                        }));
    }

    private Mono<Integer> processChunkToSendToNextStep(List<PaperDelivery> chunk, String sortKeyPrefix, String tenderId, LocalDate deliveryWeek, Integer provinceDeclaredCapacity, AtomicInteger printCounter, List<IncrementUsedCapacityDto> incrementUsedCapacities) {
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();
        String unifiedDeliveryDriver = sortKeyPrefix.split("~")[0];
        String province = sortKeyPrefix.split("~")[1];

        return evaluateCapCapacity(chunk, unifiedDeliveryDriver, tenderId, deliveryWeek, deliveriesToSend, incrementUsedCapacities)
                .doOnNext(unused -> incrementUsedCapacities.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, province, deliveriesToSend.size(), deliveryWeek, provinceDeclaredCapacity)))
                .flatMap(unused -> paperDeliveryDAO.insertPaperDeliveries(deliveriesToSend).thenReturn(deliveriesToSend.size())
                        .doOnNext(printCounter::addAndGet));
//                        .flatMap(sentToNextStepCounter -> paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryWeek, sentToNextStepCounter, weeklyPrintCapacity)
//                                .thenReturn(sentToNextStepCounter))
//                        .flatMap(sentToNextStepCounter -> deliveryDriverUtils.updateCounters(incrementCapacities)
                               // .thenReturn(sentToNextStepCounter)));
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

    private Mono<Integer> evaluateCapCapacity(List<PaperDelivery> paperDeliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, List<PaperDelivery> deliveriesToSend, List<IncrementUsedCapacityDto> incrementUsedCapacityDtos) {
        Map<String, List<PaperDelivery>> capMap = pnDelayerUtils.groupByCap(paperDeliveries);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(entry -> processCapGroup(entry.getKey(), entry.getValue(), unifiedDeliveryDriver, tenderId, deliveryWeek, deliveriesToSend, incrementUsedCapacityDtos))
                .then(Mono.just(deliveriesToSend.size()));
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
     * @param deliveriesToSend          the list to which prepared deliveries will be added
     * @param incrementUsedCapacityDtos the list to which used capacity increments will be added
     * @return a Mono containing the number of processed deliveries
     */
    private Mono<Integer> processCapGroup(String cap, List<PaperDelivery> deliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, List<PaperDelivery> deliveriesToSend, List<IncrementUsedCapacityDto> incrementUsedCapacityDtos) {
        List<PaperDelivery> toNextWeek = new ArrayList<>();
        return deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(cap, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for [{}~{}] -> availableCapacity={}, usedCapacity={}", unifiedDeliveryDriver, cap, tuple.getT1(), tuple.getT2()))
                .flatMap(capCapacityAndUsedCapacity -> Mono.just(pnDelayerUtils.filterOnResidualDriverCapacity(deliveries, capCapacityAndUsedCapacity, deliveriesToSend, toNextWeek, deliveryWeek))
                        .filter(deliveriesCount -> deliveriesCount > 0)
                        .doOnDiscard(Integer.class, unused -> log.warn("No capacity for cap={} and unifiedDeliveryDriver={}, no records will be processed", cap, unifiedDeliveryDriver))
                        .doOnNext(deliveriesCount -> incrementUsedCapacityDtos.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, cap, deliveriesCount, deliveryWeek, capCapacityAndUsedCapacity.getT1()))))
                .thenReturn(toNextWeek)
                .flatMap(toNextWeekList -> processChunkToSendToNextWeek(toNextWeekList, deliveryWeek));
    }

    public Mono<List<PaperDelivery>> insertPaperDeliveries(SenderLimitJobProcessObjects senderLimitJobProcessObjects, LocalDate deliveryWeek) {
        return paperDeliveryDAO.insertPaperDeliveries(pnDelayerUtils.mapItemForEvaluateDriverCapacityStep(senderLimitJobProcessObjects.getSendToDriverCapacityStep(), deliveryWeek))
                .thenReturn(senderLimitJobProcessObjects)
                .flatMap(unused -> paperDeliveryDAO.insertPaperDeliveries(pnDelayerUtils.mapItemForResidualCapacityStep(senderLimitJobProcessObjects.getSendToResidualCapacityStep(), deliveryWeek))
                        .thenReturn(senderLimitJobProcessObjects.getSendToDriverCapacityStep())
                        .doOnNext(paperDeliveries -> paperDeliveries.removeIf(paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1)));
    }
}

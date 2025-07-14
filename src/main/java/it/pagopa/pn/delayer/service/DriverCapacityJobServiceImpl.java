package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
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

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class DriverCapacityJobServiceImpl implements DriverCapacityJobService {

    private final PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;
    private final PaperDeliveryDAO paperDeliveryDAO;
    private final PaperDeliveryUtils paperDeliveryUtils;
    private final PnDelayerConfigs pnDelayerConfigs;


    /**
     * Starts the job to evaluate driver capacity for paper deliveries, This method evaluates residual province capacity.
     * If the residual capacity is greater than zero, it retrieves paper deliveries and processes them.
     * If the residual capacity is zero or less, it sends the deliveries to the next week.
     *
     * @param unifiedDeliveryDriver the unified delivery driver identifier
     * @param province              the province for which the capacity is evaluated
     * @param lastEvaluatedKey      the last evaluated key for pagination
     * @param startExecutionBatch   the start execution batch time
     * @param tenderId              the tender identifier
     */
    @Override
    public Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch, String tenderId) {
        LocalDate deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);

        return retrieveDeclaredAndUsedCapacity(province, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for province: [{}], unifiedDeliveryDriver: [{}] -> declared capacity={}, used capacity={}", province, unifiedDeliveryDriver, tuple.getT1(), tuple.getT2()))
                .flatMap(tuple -> {
                    String sortKeyPrefix = String.join("~", unifiedDeliveryDriver, province);
                    int residualCapacity = tuple.getT1() - tuple.getT2();
                    if (residualCapacity <= 0) {
                        log.warn("No capacity for province={} and unifiedDeliveryDriver={}, no records will be processed", province, unifiedDeliveryDriver);
                        return sendToNextWeek(sortKeyPrefix, lastEvaluatedKey, deliveryWeek);
                    } else {
                        return sendToNextStep(sortKeyPrefix, lastEvaluatedKey, tenderId, deliveryWeek, residualCapacity, tuple.getT1());
                    }
                })
                .then();
    }

    private Mono<Void> sendToNextWeek(String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, LocalDate deliveryWeek) {
        return retrievePaperDeliveries(deliveryWeek.toString(), sortKeyPrefix, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit())
                .flatMap(paperDeliveryPage -> processChunkToSendToNextWeek(paperDeliveryPage.items(), deliveryWeek)
                        .flatMap(unused -> !CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey()) ?
                                sendToNextWeek(sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), deliveryWeek) : Mono.empty()));
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
            return paperDeliveryDAO.insertPaperDeliveries(paperDeliveryUtils.toNextWeek(chunk, deliveryWeek))
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
    private Mono<Integer> sendToNextStep(String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, String tenderId, LocalDate deliveryWeek, Integer residualCapacity, Integer declaredCapacity) {
        return retrievePaperDeliveries(deliveryWeek.toString(), sortKeyPrefix, lastEvaluatedKey, Math.min(residualCapacity, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()))
                .flatMap(paperDeliveryPage -> processChunkToSendToNextStep(paperDeliveryPage.items(), sortKeyPrefix, tenderId, deliveryWeek, declaredCapacity)
                        .flatMap(sentToNextStep -> {
                            int residualCapacityAfterSending = residualCapacity - sentToNextStep;
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey())) {
                                return residualCapacityAfterSending <= 0 ?
                                        sendToNextWeek(sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), deliveryWeek).thenReturn(sentToNextStep) :
                                        sendToNextStep(sortKeyPrefix, paperDeliveryPage.lastEvaluatedKey(), tenderId, deliveryWeek, residualCapacity - sentToNextStep, declaredCapacity);
                            }
                            return Mono.empty();
                        }));
    }

    private Mono<Integer> processChunkToSendToNextStep(List<PaperDelivery> chunk, String sortKeyPrefix, String tenderId, LocalDate deliveryWeek, Integer provinceDeclaredCapacity) {
        List<IncrementUsedCapacityDto> incrementCapacities = new ArrayList<>();
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();
        String unifiedDeliveryDriver = sortKeyPrefix.split("~")[0];
        String province = sortKeyPrefix.split("~")[1];

        return evaluateCapCapacity(chunk, unifiedDeliveryDriver, tenderId, deliveryWeek, deliveriesToSend, incrementCapacities)
                .doOnNext(unused -> incrementCapacities.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, province, deliveriesToSend.size(), deliveryWeek, provinceDeclaredCapacity)))
                .flatMap(unused -> paperDeliveryDAO.insertPaperDeliveries(deliveriesToSend).thenReturn(deliveriesToSend.size())
                        .flatMap(integer -> updateCounters(incrementCapacities).thenReturn(integer)));
    }

    private Mono<Integer> evaluateCapCapacity(List<PaperDelivery> paperDeliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, List<PaperDelivery> deliveriesToSend, List<IncrementUsedCapacityDto> incrementUsedCapacityDtos) {
        Map<String, List<PaperDelivery>> capMap = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(paperDeliveries);
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
        return retrieveDeclaredAndUsedCapacity(cap, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for [{}~{}] -> availableCapacity={}, usedCapacity={}", unifiedDeliveryDriver, cap, tuple.getT1(), tuple.getT2()))
                .flatMap(capCapacityAndUsedCapacity -> Mono.just(paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, capCapacityAndUsedCapacity, deliveriesToSend, toNextWeek, deliveryWeek))
                        .filter(deliveriesCount -> deliveriesCount > 0)
                        .doOnDiscard(Integer.class, discardedCount -> log.warn("No capacity for cap={} and unifiedDeliveryDriver={}, no records will be processed", cap, unifiedDeliveryDriver))
                        .doOnNext(deliveriesCount -> incrementUsedCapacityDtos.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, cap, deliveriesCount, deliveryWeek, capCapacityAndUsedCapacity.getT1()))))
                .thenReturn(toNextWeek)
                .flatMap(toNextWeekList -> processChunkToSendToNextWeek(toNextWeek, deliveryWeek));
    }

    private Mono<Page<PaperDelivery>> retrievePaperDeliveries(String sentWeek, String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, Integer queryLimit) {
        return paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, sentWeek, sortKeyPrefix, lastEvaluatedKey, queryLimit)
                .flatMap(paperDeliveryPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryPage.items())) {
                        log.warn("No records found for unifiedDeliveryDriver-province = [{}]", sortKeyPrefix);
                        return Mono.empty();
                    }
                    return Mono.just(paperDeliveryPage);
                });
    }

    private Mono<Void> updateCounters(List<IncrementUsedCapacityDto> incrementCapacities) {
        return Flux.fromIterable(incrementCapacities)
                .flatMap(incrementUsedCapacityDto ->
                        paperDeliveryUsedCapacityDAO.updateCounter(
                                incrementUsedCapacityDto.unifiedDeliveryDriver(),
                                incrementUsedCapacityDto.geoKey(),
                                incrementUsedCapacityDto.numberOfDeliveries(),
                                incrementUsedCapacityDto.deliveryWeek(),
                                incrementUsedCapacityDto.declaredCapacity()))
                .then();
    }

    private Mono<Tuple2<Integer, Integer>> retrieveDeclaredAndUsedCapacity(String geoKey, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek) {
        return paperDeliveryUsedCapacityDAO.get(unifiedDeliveryDriver, geoKey, deliveryWeek)
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No used capacities found for unifiedDeliveryDriver={}, geoKey={}, deliveryWeek={}, retrieving declared capacity", unifiedDeliveryDriver, geoKey, deliveryWeek);
                    return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geoKey, deliveryWeek)
                            .map(capacity -> Tuples.of(capacity, 0));
                }));
    }

}

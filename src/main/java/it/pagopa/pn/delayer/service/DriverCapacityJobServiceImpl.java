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

    @Override
    public Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch, String tenderId) {
        LocalDate deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);
        LocalDate sentWeek = paperDeliveryUtils.calculateSentWeek(deliveryWeek);

        return retrieveDeclaredAndUsedCapacity(province, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for province: [{}], unifiedDeliveryDriver: [{}] -> availableCapacity={}, usedCapacity={}", province, unifiedDeliveryDriver, tuple.getT1(), tuple.getT2()))
                .filter(tuple -> tuple.getT1() - tuple.getT2() > 0)
                .doOnDiscard(Integer.class, residualCapacity -> log.warn("No capacity for province={} and unifiedDeliveryDriver={}, no records will be processed", province, unifiedDeliveryDriver))
                .flatMap(tuple -> retrievePaperDeliveriesAndProcess(unifiedDeliveryDriver, province, lastEvaluatedKey, sentWeek, tenderId, deliveryWeek, tuple.getT1() - tuple.getT2(), tuple.getT1()))
                .then();
    }

    private Mono<Integer> retrievePaperDeliveriesAndProcess(String unifiedDeliveryDriver, String province, Map<String, AttributeValue> lastEvaluatedKey, LocalDate sentWeek, String tenderId, LocalDate deliveryWeek, Integer residualCapacity, Integer declaredCapacity) {
        return paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, sentWeek.toString(), String.join("~", unifiedDeliveryDriver, province), lastEvaluatedKey, Math.min(residualCapacity, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()))
                .flatMap(paperDeliveryPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryPage.items())) {
                        log.warn("No records found for unifiedDeliveryDriver={} and province:{}", unifiedDeliveryDriver, province);
                        return Mono.empty();
                    }
                    return processChunk(paperDeliveryPage.items(), unifiedDeliveryDriver, tenderId, province, deliveryWeek, declaredCapacity)
                            .flatMap(sentToNextStep -> {
                                if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey()) && (residualCapacity - sentToNextStep) > 0) {
                                    return retrievePaperDeliveriesAndProcess(unifiedDeliveryDriver, province, paperDeliveryPage.lastEvaluatedKey(), sentWeek, tenderId, deliveryWeek, residualCapacity - sentToNextStep, declaredCapacity);
                                } else {
                                    return Mono.empty();
                                }
                            });
                });
    }

    private Mono<Integer> processChunk(List<PaperDelivery> chunk, String unifiedDeliveryDriver, String tenderId, String province, LocalDate deliveryWeek, Integer provinceDeclaredCapacity) {
        List<IncrementUsedCapacityDto> incrementCapacities = new ArrayList<>();
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();

        return evaluateCapCapacity(chunk, unifiedDeliveryDriver, tenderId, deliveryWeek, deliveriesToSend, incrementCapacities)
                .doOnNext(unused -> incrementCapacities.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, province, deliveriesToSend.size(), deliveryWeek, provinceDeclaredCapacity)))
                .flatMap(unused -> paperDeliveryDAO.insertPaperDeliveries(deliveriesToSend)
                        .thenReturn(deliveriesToSend.size())
                        .flatMap(integer -> updateCounters(incrementCapacities)
                                .thenReturn(integer)));
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

    private Mono<Integer> evaluateCapCapacity(List<PaperDelivery> paperDeliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, List<PaperDelivery> deliveriesToSend, List<IncrementUsedCapacityDto> incrementUsedCapacityDtos) {
        Map<String, List<PaperDelivery>> capMap = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(paperDeliveries);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(entry -> processCapGroup(entry.getKey(), entry.getValue(), unifiedDeliveryDriver, tenderId, deliveryWeek, deliveriesToSend, incrementUsedCapacityDtos))
                .then(Mono.just(deliveriesToSend.size()));
    }

    private Mono<Integer> processCapGroup(String cap, List<PaperDelivery> deliveries, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek, List<PaperDelivery> deliveriesToSend, List<IncrementUsedCapacityDto> incrementUsedCapacityDtos) {
        return retrieveDeclaredAndUsedCapacity(cap, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for [{}~{}] -> availableCapacity={}, usedCapacity={}",
                        unifiedDeliveryDriver, cap, tuple.getT1(), tuple.getT2()))
                .flatMap(capCapacityAndUsedCapacity -> Mono.just(paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, capCapacityAndUsedCapacity, deliveriesToSend))
                        .filter(deliveriesCount -> deliveriesCount > 0)
                        .doOnNext(deliveriesCount -> incrementUsedCapacityDtos.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, cap, deliveriesCount, deliveryWeek, capCapacityAndUsedCapacity.getT1())))
                        .doOnDiscard(Integer.class, discardedCount -> log.warn("No capacity for cap={} and unifiedDeliveryDriver={}, no records will be processed", cap, unifiedDeliveryDriver)));
    }

    private Mono<Tuple2<Integer, Integer>> retrieveDeclaredAndUsedCapacity(String geoKey, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek) {
        return paperDeliveryUsedCapacityDAO.get(unifiedDeliveryDriver, geoKey, deliveryWeek)
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("No used capacities found for unifiedDeliveryDriver={}, geoKey={}, deliveryWeek={}", unifiedDeliveryDriver, geoKey, deliveryWeek);
                    return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geoKey, deliveryWeek)
                            .map(capacity -> Tuples.of(capacity, 0));
                }));
    }
}

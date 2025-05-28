package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import it.pagopa.pn.delayer.model.PaperDeliveryTransactionRequest;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HighPriorityBatchServiceImpl implements HighPriorityBatchService {

    private final PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;
    private final PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDAO;
    private final PaperDeliveryUtils paperDeliveryUtils;

    @Override
    public Mono<Void> initHighPriorityBatch(String pk, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch) {
        return paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(PaperDeliveryHighPriority.retrieveunifiedDeliveryDriver(pk),
                        PaperDeliveryHighPriority.retrieveGeoKey(pk), lastEvaluatedKey)
                .flatMap(paperDeliveryHighPriorityPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.items())) {
                        log.warn("No high priority records found for pk={}", pk);
                        return Mono.empty();
                    }
                    return processChunk(paperDeliveryHighPriorityPage.items(), startExecutionBatch)
                            .flatMap(unused -> {
                                if (!CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.lastEvaluatedKey())) {
                                    return initHighPriorityBatch(pk, paperDeliveryHighPriorityPage.lastEvaluatedKey(), startExecutionBatch);
                                } else {
                                    return Mono.empty();
                                }
                            });
                });
    }

    private Mono<List<PaperDeliveryHighPriority>> processChunk(List<PaperDeliveryHighPriority> chunk, Instant startExecutionBatch) {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        Instant deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);
        PaperDeliveryHighPriority paperDeliveryHighPriority = chunk.get(0);
        var incrementCapacities = new ArrayList<IncrementUsedCapacityDto>();
        return retrieveCapacities(paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getUnifiedDeliveryDriver(), paperDeliveryHighPriority.getTenderId(), deliveryWeek)
                .map(tuple -> paperDeliveryUtils.checkCapacityAndFilterList(tuple, chunk))
                .filter(filteredChunk -> !CollectionUtils.isEmpty(filteredChunk))
                .doOnDiscard(List.class, filteredChunk -> log.warn("No capacity for province={} and unifiedDeliveryDriver={}, no records will be processed", paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getUnifiedDeliveryDriver()))
                .flatMap(filteredChunk -> evaluateCapCapacity(filteredChunk, paperDeliveryHighPriority.getUnifiedDeliveryDriver(), paperDeliveryHighPriority.getTenderId(), paperDeliveryHighPriority.getProvince(), deliveryWeek, transactionRequest, incrementCapacities))
                .filter(paperDeliveryUtils::checkListsSize)
                .flatMap(unused -> paperDeliveryHighPriorityDAO.executeTransaction(transactionRequest.getPaperDeliveryHighPriorityList(), transactionRequest.getPaperDeliveryReadyToSendList()).thenReturn(chunk))
                .flatMap(unused -> updateCounters(incrementCapacities))
                .thenReturn(chunk);
    }

    private Mono<Void> updateCounters(List<IncrementUsedCapacityDto> incrementCapacities) {
        return Flux.fromIterable(incrementCapacities)
                .flatMap(incrementUsedCapacityDto ->
                        paperDeliveryUsedCapacityDAO.updateCounter(
                                incrementUsedCapacityDto.unifiedDeliveryDriver(),
                                incrementUsedCapacityDto.geoKey(),
                                incrementUsedCapacityDto.numberOfDeliveries(),
                                incrementUsedCapacityDto.deliveryWeek()))
                .then();
    }

    private Mono<PaperDeliveryTransactionRequest> evaluateCapCapacity(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities, String unifiedDeliveryDriver, String tenderId, String province, Instant deliveryWeek, PaperDeliveryTransactionRequest transactionRequest, List<IncrementUsedCapacityDto> incrementCapacities) {
        Map<String, List<PaperDeliveryHighPriority>> capMap = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(paperDeliveryHighPriorities);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(entry -> processCapGroupAndUpdateCounter(entry.getKey(), entry.getValue(), unifiedDeliveryDriver, tenderId, deliveryWeek, transactionRequest, incrementCapacities))
                .reduce(0, Integer::sum)
                .filter(numberOfDeliveries -> numberOfDeliveries > 0)
//                .flatMap(numberOfDeliveries -> paperDeliveryUsedCapacityDAO.updateCounter(unifiedDeliveryDriver, province, numberOfDeliveries, deliveryWeek))
                .doOnNext(numberOfDeliveries -> incrementCapacities.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, province, numberOfDeliveries, deliveryWeek)))
                .then(Mono.just(transactionRequest));
    }

    private Mono<Integer> processCapGroupAndUpdateCounter(String cap, List<PaperDeliveryHighPriority> deliveries, String unifiedDeliveryDriver, String tenderId, Instant deliveryWeek, PaperDeliveryTransactionRequest transactionRequest, List<IncrementUsedCapacityDto> incrementCapacities) {
        return retrieveCapacities(cap, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .map(registryCapacityAndUsedCapacity -> paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, registryCapacityAndUsedCapacity))
                .filter(numberOfDeliveries -> numberOfDeliveries > 0)
                .doOnDiscard(Integer.class, numberOfDeliveries -> log.warn("No capacity for cap={} and unifiedDeliveryDriver={}, no records will be processed", cap, unifiedDeliveryDriver))
//                .flatMap(numberOfDeliveries -> paperDeliveryUsedCapacityDAO.updateCounter(unifiedDeliveryDriver, cap, numberOfDeliveries, deliveryWeek));
                .doOnNext(numberOfDeliveries -> incrementCapacities.add(new IncrementUsedCapacityDto(unifiedDeliveryDriver, cap, numberOfDeliveries, deliveryWeek)));
    }

    private Mono<Tuple2<Integer, Integer>> retrieveCapacities(String geoKey, String unifiedDeliveryDriver, String tenderId, Instant deliveryWeek) {
        return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geoKey, deliveryWeek)
                .zipWith(paperDeliveryUsedCapacityDAO.get(unifiedDeliveryDriver, geoKey, deliveryWeek));
    }
}

package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
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
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HighPriorityBatchServiceImpl implements HighPriorityBatchService {

    private final PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDispatchedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;
    private final PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDAO;
    private final PaperDeliveryUtils paperDeliveryUtils;

    @Override
    public Mono<Void> initHighPriorityBatch(String pk, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch) {
        return paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(PaperDeliveryHighPriority.retrieveDeliveryDriverId(pk),
                        PaperDeliveryHighPriority.retrieveGeoKey(pk), lastEvaluatedKey)
                .flatMap(paperDeliveryHighPriorityPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.items())) {
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
        Instant deliveryWeek = paperDeliveryUtils.calculateNextWeek(startExecutionBatch);
        PaperDeliveryHighPriority paperDeliveryHighPriority = chunk.get(0);
        return retrieveCapacities(paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), deliveryWeek)
                .map(tuple -> paperDeliveryUtils.checkCapacityAndFilterList(tuple, chunk))
                .filter(filteredChunk -> !CollectionUtils.isEmpty(filteredChunk))
                .doOnDiscard(List.class, filteredChunk -> log.warn("No capacity for province={} and deliveryDriverId={}, no records will be processed", paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getDeliveryDriverId()))
                .flatMap(filteredChunk -> evaluateCapCapacity(filteredChunk, paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), paperDeliveryHighPriority.getProvince(), deliveryWeek, transactionRequest))
                .filter(paperDeliveryUtils::checkListsSize)
                .flatMap(unused -> paperDeliveryHighPriorityDAO.executeTransaction(transactionRequest.getPaperDeliveryHighPriorityList(), transactionRequest.getPaperDeliveryReadyToSendList()))
                .thenReturn(chunk);
    }

    private Mono<PaperDeliveryTransactionRequest> evaluateCapCapacity(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities, String deliveryDriverId, String tenderId, String province, Instant deliveryWeek, PaperDeliveryTransactionRequest transactionRequest) {
        Map<String, List<PaperDeliveryHighPriority>> capMap = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(paperDeliveryHighPriorities);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(entry -> processCapGroupAndUpdateCounter(entry.getKey(), entry.getValue(), deliveryDriverId, tenderId, deliveryWeek, transactionRequest))
                .reduce(0, Integer::sum)
                .flatMap(numberOfDeliveries -> paperDeliveryDispatchedCapacityDAO.updateCounter(deliveryDriverId, province, numberOfDeliveries, deliveryWeek))
                .then(Mono.just(transactionRequest));
    }

    private Mono<Integer> processCapGroupAndUpdateCounter(String cap, List<PaperDeliveryHighPriority> deliveries, String deliveryDriverId, String tenderId, Instant deliveryWeek, PaperDeliveryTransactionRequest transactionRequest) {
        return retrieveCapacities(cap, deliveryDriverId, tenderId, deliveryWeek)
                .map(registryCapacityAndUsedCapacity -> paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, registryCapacityAndUsedCapacity))
                .filter(numberOfDeliveries -> numberOfDeliveries > 0)
                .doOnDiscard(Integer.class, numberOfDeliveries -> log.warn("No capacity for cap={} and deliveryDriverId={}, no records will be processed", cap, deliveryDriverId))
                .flatMap(numberOfDeliveries -> paperDeliveryDispatchedCapacityDAO.updateCounter(deliveryDriverId, cap, numberOfDeliveries, deliveryWeek));
    }

    private Mono<Tuple2<Integer, Integer>> retrieveCapacities(String geoKey, String deliveryDriverId, String tenderId, Instant deliveryWeek) {
        return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryWeek)
                .zipWith(paperDeliveryDispatchedCapacityDAO.get(deliveryDriverId, geoKey, deliveryWeek));
    }
}

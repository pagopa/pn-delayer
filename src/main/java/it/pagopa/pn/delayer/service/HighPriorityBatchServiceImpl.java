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
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

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
    public Mono<Void> initHighPriorityBatch(String pk, Map<String, AttributeValue> lastEvaluatedKey) {
        return paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(PaperDeliveryHighPriority.retrieveDeliveryDriverId(pk),
                        PaperDeliveryHighPriority.retrieveGeoKey(pk), lastEvaluatedKey)
                .flatMap(paperDeliveryHighPriorityPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.items())) {
                        return Mono.empty();
                    }
                    return processChunk(paperDeliveryHighPriorityPage.items())
                            .flatMap(unused -> {
                                if (!CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.lastEvaluatedKey())) {
                                    return initHighPriorityBatch(pk, paperDeliveryHighPriorityPage.lastEvaluatedKey());
                                } else {
                                    return Mono.empty();
                                }
                            });
                });
    }

    private Mono<List<PaperDeliveryHighPriority>> processChunk(List<PaperDeliveryHighPriority> chunk) {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        Instant deliveryWeek = paperDeliveryUtils.calculateNextWeek(Instant.now());
        PaperDeliveryHighPriority paperDeliveryHighPriority = chunk.get(0);
        return retrieveCapacities(paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), deliveryWeek)
                .map(tuple -> paperDeliveryUtils.checkCapacityAndFilterList(tuple, chunk))
                .filter(filteredChunk -> !CollectionUtils.isEmpty(filteredChunk))
                .flatMap(filteredChunk -> evaluateCapCapacity(filteredChunk, paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), paperDeliveryHighPriority.getProvince(), deliveryWeek, transactionRequest))
                .filter(paperDeliveryUtils::checkListsSize)
                .flatMap(unused -> paperDeliveryHighPriorityDAO.executeTransaction(transactionRequest.getPaperDeliveryHighPriorityList(), transactionRequest.getPaperDeliveryReadyToSendList()))
                .thenReturn(chunk);
    }

    private Mono<PaperDeliveryTransactionRequest> evaluateCapCapacity(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities, String deliveryDriverId, String tenderId, String province, Instant deliveryWeek, PaperDeliveryTransactionRequest transactionRequest) {
        Map<String, List<PaperDeliveryHighPriority>> capMap = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(paperDeliveryHighPriorities);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(entry -> processCapGroupAndUpdateCounter(entry.getKey(), entry.getValue(), deliveryDriverId, tenderId, province, deliveryWeek, transactionRequest))
                .then(Mono.just(transactionRequest));
    }

    private Mono<UpdateItemResponse> processCapGroupAndUpdateCounter(String cap, List<PaperDeliveryHighPriority> deliveries, String deliveryDriverId, String tenderId, String province, Instant deliveryWeek, PaperDeliveryTransactionRequest transactionRequest) {
        return retrieveCapacities(cap, deliveryDriverId, tenderId, deliveryWeek)
                .map(tuple -> paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, tuple))
                .filter(numberOfDeliveries -> numberOfDeliveries > 0)
                .flatMap(numberOfDeliveries -> paperDeliveryDispatchedCapacityDAO.updateCounter(deliveryDriverId, cap, numberOfDeliveries, deliveryWeek)
                        .flatMap(updateItemResponse -> paperDeliveryDispatchedCapacityDAO.updateCounter(deliveryDriverId, province, numberOfDeliveries, deliveryWeek)));
    }

    private Mono<Tuple2<Integer, Integer>> retrieveCapacities(String geoKey, String deliveryDriverId, String tenderId, Instant deliveryWeek) {
        return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryWeek)
                .zipWith(paperDeliveryDispatchedCapacityDAO.get(deliveryDriverId, geoKey, deliveryWeek));
    }
}

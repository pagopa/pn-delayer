package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
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

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class HighPriorityServiceImpl implements HighPriorityBatchService {

    private final PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDispatchedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;
    private final PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDAO;
    private final PaperDeliveryUtils paperDeliveryUtils;

    @Override
    public Mono<Integer> initHighPriorityBatch(String pk) {
        return paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(PaperDeliveryHighPriority.retrieveDeliveryDriverId(pk),
                        PaperDeliveryHighPriority.retrieveGeoKey(pk),  new HashMap<>())
                .flatMap(paperDeliveryHighPriorityPage -> {
                    if (CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.items())) {
                        return Mono.empty();
                    }
                    return processChunk(paperDeliveryHighPriorityPage.items())
                            .flatMap(aBool -> {
                                if (!CollectionUtils.isEmpty(paperDeliveryHighPriorityPage.lastEvaluatedKey())) {
                                    return initHighPriorityBatch(pk);
                                } else {
                                    return Mono.empty();
                                }
                            });
                });
    }

    private Mono<PaperDeliveryTransactionRequest> processChunk(List<PaperDeliveryHighPriority> chunk) {
        Instant deliveryWeek = paperDeliveryUtils.calculateNextWeek(Instant.now());
        PaperDeliveryHighPriority paperDeliveryHighPriority = chunk.get(0);
        return evaluateCapacity(paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), deliveryWeek)
                .map(tuple -> checkCapacityAndFilterList(tuple, chunk))
                .flatMap(filteredChunk -> evaluateCapCapacityAndWrite(filteredChunk, paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(),
                                paperDeliveryHighPriority.getProvince(), deliveryWeek));
    }

    private Mono<PaperDeliveryTransactionRequest> evaluateCapCapacityAndWrite(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities, String deliveryDriverId, String tenderId, String province, Instant deliveryWeek) {
        PaperDeliveryTransactionRequest deliveryRequestToSend = new PaperDeliveryTransactionRequest();
        Map<String, List<PaperDeliveryHighPriority>> capMap = groupDeliveryOnCap(paperDeliveryHighPriorities);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(stringListEntry -> evaluateCapacity(stringListEntry.getKey(), deliveryDriverId, tenderId, deliveryWeek)
                        .flatMap(tuple -> {
                            List<PaperDeliveryHighPriority> filteredList = checkCapacityAndFilterList(tuple, stringListEntry.getValue());
                            deliveryRequestToSend.getPaperDeliveryHighPriorityList().addAll(filteredList);
                            deliveryRequestToSend.getPaperDeliveryReadyToSendList().addAll(paperDeliveryUtils.mapToPaperDeliveryReadyToSend(filteredList, tuple.getT1(), tuple.getT2()));
                            return paperDeliveryDispatchedCapacityDAO.updateCounter(deliveryDriverId, stringListEntry.getKey(),filteredList.size(), deliveryWeek)
                                    .flatMap(updatedCapCapacity -> paperDeliveryDispatchedCapacityDAO.updateCounter(deliveryDriverId, province,filteredList.size(), deliveryWeek));
                        }))
                .collectList()
                .thenReturn(deliveryRequestToSend)
                .filter(paperDeliveryTransactionRequest -> !CollectionUtils.isEmpty(paperDeliveryTransactionRequest.getPaperDeliveryHighPriorityList()) &&
                        !CollectionUtils.isEmpty(paperDeliveryTransactionRequest.getPaperDeliveryReadyToSendList()))
                .flatMap(paperDeliveryTransactionRequest -> paperDeliveryHighPriorityDAO.executeTransaction(paperDeliveryTransactionRequest.getPaperDeliveryHighPriorityList(),
                                paperDeliveryTransactionRequest.getPaperDeliveryReadyToSendList())
                .thenReturn(deliveryRequestToSend));

    }

    private List<PaperDeliveryHighPriority> checkCapacityAndFilterList(Tuple2<Integer, Integer> tuple, List<PaperDeliveryHighPriority> paperDeliveryHighPriorities) {
        int remainingCapacity = tuple.getT2() == 0 ? tuple.getT1() : tuple.getT1() - tuple.getT2();
        if (remainingCapacity == 0) {
            return Collections.emptyList();
        } else {
            return paperDeliveryHighPriorities.size() < remainingCapacity ? paperDeliveryHighPriorities : paperDeliveryHighPriorities.stream().limit(remainingCapacity).toList();
        }
    }

    private static Map<String, List<PaperDeliveryHighPriority>> groupDeliveryOnCap(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities) {
        return paperDeliveryHighPriorities.stream()
                .collect(Collectors.groupingBy(
                        PaperDeliveryHighPriority::getCap,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(PaperDeliveryHighPriority::getCreatedAt));
                                    return list;
                                }
                        )
                ));
    }

    private Mono<Tuple2<Integer, Integer>> evaluateCapacity(String geoKey, String deliveryDriverId, String tenderId, Instant deliveryWeek) {
        return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryWeek)
                .zipWith(paperDeliveryDispatchedCapacityDAO.get(deliveryDriverId, geoKey, deliveryWeek));
    }
}

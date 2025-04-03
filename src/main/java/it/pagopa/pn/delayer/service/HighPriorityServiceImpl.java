package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfig;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryReadyToSendDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryDriverCapacities;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class HighPriorityServiceImpl implements HighPriorityBatchService {

    private final PaperDeliveryReadyToSendDAO paperDeliveryReadyToSendDAO;
    private final PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDispatchedCapacityInMemoryDb;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityInMemoryDb;
    private final PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDAO;
    private final PnDelayerConfig pnDelayerConfig;

    @Override
    public Mono<Void> initHighPriorityBatch(String pk) {
        return paperDeliveryHighPriorityDAO.getChunck(pk, pnDelayerConfig.getHighPriorityQuerySize(), new HashMap<>())
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

    private Mono<Integer> processChunk(List<PaperDeliveryHighPriority> chunk) {
        PaperDeliveryHighPriority paperDeliveryHighPriority = chunk.get(0);
        return evaluateCapacity(paperDeliveryHighPriority.getProvince(), paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId())
                .flatMap(provinceCapacity -> {
                    if (provinceCapacity != 0 && chunk.size() < provinceCapacity) {
                        return evaluateCapCapacityAndWrite(chunk, paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), paperDeliveryHighPriority.getProvince());
                    } else {
                        var finalList = chunk.stream().limit(provinceCapacity).toList();
                        log.info("Writing papers {}", finalList.size());
                        return evaluateCapCapacityAndWrite(finalList, paperDeliveryHighPriority.getDeliveryDriverId(), paperDeliveryHighPriority.getTenderId(), paperDeliveryHighPriority.getProvince());

                    }
                });
    }

    private Mono<Integer> evaluateCapCapacityAndWrite(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities, String deliveryDriverId, String tenderId, String province) {
        List<PaperDeliveryHighPriority> deliveryRequestToSend = new ArrayList<>();
        Map<String, List<PaperDeliveryHighPriority>> capMap = groupDeliveryOnCap(paperDeliveryHighPriorities);
        return Flux.fromIterable(capMap.entrySet())
                .flatMap(stringListEntry -> evaluateCapacity(stringListEntry.getKey(), deliveryDriverId, tenderId)
                        .filter(capCapacity -> capCapacity != 0)
                        .map(capCapacity -> stringListEntry.getValue().size() < capCapacity ? stringListEntry.getValue(): stringListEntry.getValue().stream().limit(capCapacity).toList())
                        .filter(paperDeliveryList -> !CollectionUtils.isEmpty(paperDeliveryList))
                        .flatMap(paperDeliveryList -> {
                                deliveryRequestToSend.addAll(paperDeliveryList);
                                return paperDeliveryDispatchedCapacityInMemoryDb.update(deliveryDriverId, stringListEntry.getKey(), tenderId, paperDeliveryList.size())
                                        .flatMap(updatedCapCapacity -> paperDeliveryDispatchedCapacityInMemoryDb.update(deliveryDriverId, province, tenderId, deliveryRequestToSend.size()));
                        }))
                .collectList()
                .thenReturn(deliveryRequestToSend)
                .filter(aInt -> !CollectionUtils.isEmpty(deliveryRequestToSend))
                .flatMap(aInt -> paperDeliveryReadyToSendDAO.executeTransaction(deliveryRequestToSend));
    }

    private static Map<String, List<PaperDeliveryHighPriority>> groupDeliveryOnCap(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities) {
        return paperDeliveryHighPriorities.stream()
                .collect(Collectors.groupingBy(
                        PaperDeliveryHighPriority::getCap,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(item -> Instant.parse(item.getCreatedAt())));
                                    return list;
                                }
                        )
                ));
    }

    private Mono<Integer> evaluateCapacity(String geoKey, String deliveryDriverId, String tenderId) {
        return paperDeliveryCapacityInMemoryDb.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey)
                .zipWith(paperDeliveryDispatchedCapacityInMemoryDb.get(deliveryDriverId, geoKey))
                .map(tuple -> tuple.getT2() == 0 ? tuple.getT1() : tuple.getT1() - tuple.getT2());
    }
}

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
@RequiredArgsConstructor
public class SenderLimitUtils {

    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final PnDelayerUtils pnDelayerUtils;

    public Mono<Void> retrieveAndEvaluateSenderLimit(LocalDate deliveryWeek, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, Map<String, Tuple2<Integer, Integer>> senderLimitMap, Integer capacity, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        return retrieveUsedSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMap)
                .thenReturn(senderLimitMap)
                .flatMap(unused -> retrieveAndCalculateSenderLimit(deliveryWeek, capacity, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMap))
                .doOnNext(residualSenderLimitMaps -> pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(senderLimitMap, deliveriesGroupedByProductTypePaId, senderLimitJobPaperDeliveries));
    }

    private Mono<Void> retrieveUsedSenderLimit(LocalDate deliveryWeek, Set<String> paIdProductTypeTuples, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        return Flux.fromIterable(paIdProductTypeTuples).buffer(25)
                .flatMap(usedSenderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(usedSenderLimitPkSubList, deliveryWeek)
                        .doOnNext(paperDeliveryUsedSenderLimit -> {
                            senderLimitMap.put(paperDeliveryUsedSenderLimit.getPk(), Tuples.of(paperDeliveryUsedSenderLimit.getSenderLimit(), paperDeliveryUsedSenderLimit.getNumberOfShipment()));
                        }))
                .then();
    }

    private Mono<Void> retrieveAndCalculateSenderLimit(LocalDate deliveryDate, Integer declaredCapacity, Set<String> paIdProductTypeTuples, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        List<String> paIdProductTypeTuplesCopy = new ArrayList<>(paIdProductTypeTuples);
        paIdProductTypeTuplesCopy.removeIf(senderLimitMap::containsKey);
        return Flux.fromIterable(paIdProductTypeTuplesCopy).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, deliveryDate)
                        .doOnNext(paperDeliverySenderLimit -> {
                            Integer limit = (declaredCapacity * paperDeliverySenderLimit.getPercentageLimit()) / 100; //LIMITE ARROTONDATO PER DIFETTO
                            senderLimitMap.put(paperDeliverySenderLimit.getPk(), Tuples.of(limit,0));
                        }))
                .then();
    }

    public Mono<Long> updateUsedSenderLimit(List<PaperDelivery> paperDeliveryList, LocalDate deliveryDate, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        Map<String, Long> usedSenderLimitMap = pnDelayerUtils.groupByPaIdProductTypeProvinceAndCount(paperDeliveryList);
        return Flux.fromIterable(usedSenderLimitMap.entrySet())
                .flatMap(tupleCounterEntry -> paperDeliverySenderLimitDAO.updateUsedSenderLimit(tupleCounterEntry.getKey(), tupleCounterEntry.getValue(), deliveryDate, senderLimitMap.get(tupleCounterEntry.getKey()).getT1()))
                .reduce(0L, Long::sum);
    }
}

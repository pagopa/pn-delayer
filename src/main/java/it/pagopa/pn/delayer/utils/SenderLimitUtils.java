package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.SenderLimitMaps;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
@RequiredArgsConstructor
public class SenderLimitUtils {

    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final PnDelayerUtils pnDelayerUtils;

    public Mono<Void> retrieveAndEvaluateSenderLimit(LocalDate deliveryWeek, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, SenderLimitMaps senderLimitMaps, Integer capacity, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        return retrieveResidualSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMaps)
                .thenReturn(senderLimitMaps)
                .flatMap(unused -> calculateSenderLimit(deliveryWeek, capacity, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMaps))
                .thenReturn(senderLimitMaps)
                .map(residualSenderLimitMaps -> pnDelayerUtils.evaluateSenderLimitAndFilter(senderLimitMaps.getResidualSenderLimitMap(), deliveriesGroupedByProductTypePaId, senderLimitJobPaperDeliveries))
                .then();
    }

    private Mono<Void> retrieveResidualSenderLimit(LocalDate deliveryWeek, Set<String> paIdProductTypeTuples, SenderLimitMaps senderLimitMaps) {
        return Flux.fromIterable(paIdProductTypeTuples).buffer(25)
                .flatMap(usedSenderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(usedSenderLimitPkSubList, deliveryWeek)
                        .doOnNext(paperDeliveryUsedSenderLimit -> {
                            senderLimitMaps.getSenderLimitMap().put(paperDeliveryUsedSenderLimit.getPk(), paperDeliveryUsedSenderLimit.getSenderLimit());
                            senderLimitMaps.getResidualSenderLimitMap().put(paperDeliveryUsedSenderLimit.getPk(), paperDeliveryUsedSenderLimit.getSenderLimit() - paperDeliveryUsedSenderLimit.getNumberOfShipment());
                        }))
                .then();
    }

    private Mono<Void> calculateSenderLimit(LocalDate deliveryDate, Integer declaredCapacity, Set<String> paIdProductTypeTuples, SenderLimitMaps senderLimitMaps) {
        paIdProductTypeTuples.removeIf(senderLimitPk -> senderLimitMaps.getSenderLimitMap().containsKey(senderLimitPk));
        return Flux.fromIterable(paIdProductTypeTuples).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, deliveryDate)
                        .doOnNext(paperDeliverySenderLimit -> {
                            Integer limit = (declaredCapacity * paperDeliverySenderLimit.getPercentageLimit()) / 100;
                            senderLimitMaps.getResidualSenderLimitMap().put(paperDeliverySenderLimit.getPk(), limit);
                            senderLimitMaps.getSenderLimitMap().put(paperDeliverySenderLimit.getPk(), limit);
                        }))
                .then();
    }

    public Mono<Integer> updateUsedSenderLimit(List<PaperDelivery> paperDeliveryList, LocalDate deliveryDate, Map<String, Integer> senderLimitMap) {
        Map<String, Long> usedSenderLimitMap = pnDelayerUtils.groupByPaIdProductTypeProvinceAndCount(paperDeliveryList);
        return Flux.fromIterable(usedSenderLimitMap.entrySet())
                .flatMap(tupleCounterEntry -> paperDeliverySenderLimitDAO.updateUsedSenderLimit(tupleCounterEntry.getKey(), tupleCounterEntry.getValue().intValue(), deliveryDate, senderLimitMap.get(tupleCounterEntry.getKey())))
                .reduce(0, Integer::sum);
    }
}

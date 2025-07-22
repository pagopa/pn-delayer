package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.LocalDate;
import java.util.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class SenderLimitUtils {

    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final PnDelayerUtils pnDelayerUtils;

    public Mono<SenderLimitJobPaperDeliveries> retrieveAndEvaluateSenderLimit(LocalDate deliveryWeek, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, Map<String, Tuple2<Integer, Integer>> senderLimitMap, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        return retrieveUsedSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMap)
                .thenReturn(senderLimitMap)
                .flatMap(unused -> retrieveAndCalculateSenderLimit(deliveryWeek, driversTotalCapacity, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMap))
                .thenReturn(senderLimitMap)
                .doOnNext(limitMap -> pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(limitMap, deliveriesGroupedByProductTypePaId, senderLimitJobPaperDeliveries))
                .thenReturn(senderLimitJobPaperDeliveries);
    }

    /**
     * Retrieves the used sender limit for the given delivery week and product type tuples.
     * The results are stored in the provided senderLimitMap.
     * @param deliveryWeek LocalDate representing the delivery date
     * @param paIdProductTypeTuples Set of tuples representing paId~product
     * @param senderLimitMap Map containing limit for each paId~productType tuple
     * */
    private Mono<Void> retrieveUsedSenderLimit(LocalDate deliveryWeek, Set<String> paIdProductTypeTuples, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        return Flux.fromIterable(paIdProductTypeTuples).buffer(25)
                .flatMap(usedSenderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(usedSenderLimitPkSubList, deliveryWeek)
                        .doOnNext(paperDeliveryUsedSenderLimit -> senderLimitMap.put(paperDeliveryUsedSenderLimit.getPk(), Tuples.of(paperDeliveryUsedSenderLimit.getSenderLimit(), paperDeliveryUsedSenderLimit.getNumberOfShipment()))))
                .then();
    }

    /**
     * Retrieves the sender limits for the specified delivery date only for the paIdProductType entries
     * that are not already present in the provided senderLimitMap.
     * For each retrieved entry, calculates the limit based on the corresponding drivers' total capacity.
     * The computed limits are then stored in the senderLimitMap.
     * @param deliveryDate The date for which the sender limits are to be retrieved
     * @param driversTotalCapacity List containing calculated capacities for each products or list of products and related unifiedDeliveryDrivers
     * @param paIdProductTypeTuples Set of tuple of paId~productType
     * @param senderLimitMap Map containing calculated limit for each paId~productType tuple
     */
    private Mono<Void> retrieveAndCalculateSenderLimit(LocalDate deliveryDate, List<DriversTotalCapacity> driversTotalCapacity, Set<String> paIdProductTypeTuples, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        List<String> paIdProductTypeTuplesCopy = new ArrayList<>(paIdProductTypeTuples);
        paIdProductTypeTuplesCopy.removeIf(senderLimitMap::containsKey);
        return Flux.fromIterable(paIdProductTypeTuplesCopy).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, deliveryDate)
                        .map(paperDeliverySenderLimit -> {
                            int declaredCapacity = driversTotalCapacity.stream()
                                    .filter(driver -> driver.getProducts().contains(paperDeliverySenderLimit.getProductType()))
                                    .findFirst()
                                    .map(DriversTotalCapacity::getCapacity)
                                    .orElse(0);
                            Integer limit = (declaredCapacity * paperDeliverySenderLimit.getPercentageLimit()) / 100; //LIMITE ARROTONDATO PER DIFETTO
                            senderLimitMap.put(paperDeliverySenderLimit.getPk(), Tuples.of(limit,0));
                            return senderLimitMap;
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

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.IncrementUsedSenderLimitDto;
import it.pagopa.pn.delayer.model.ProductType;
import it.pagopa.pn.delayer.model.SenderLimitJobProcessObjects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
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
    private final PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    public Mono<SenderLimitJobProcessObjects> retrieveAndEvaluateSenderLimit(LocalDate deliveryWeek, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        LocalDate shipmentDate = deliveryWeek.minusWeeks(1);
        return retrieveUsedSenderLimit(shipmentDate, deliveriesGroupedByProductTypePaId.keySet(), senderLimitJobProcessObjects.getSenderLimitMap())
                .flatMap(unused -> retrieveAndCalculateSenderLimit(shipmentDate, driversTotalCapacity, deliveriesGroupedByProductTypePaId.keySet(), senderLimitJobProcessObjects))
                .doOnNext(unused -> pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(senderLimitJobProcessObjects.getSenderLimitMap(), deliveriesGroupedByProductTypePaId, senderLimitJobProcessObjects))
                .thenReturn(senderLimitJobProcessObjects);
    }

    public Mono<Map<String,Integer>> retrieveTotalEstimateCounter(LocalDate deliveryWeek, String province){
        Map<String,Integer> totalEstimateCounter = new HashMap<>();
        return Flux.fromStream(Arrays.stream(ProductType.values()))
                .flatMap(product -> {
                    String sk = PaperDeliveryCounter.buildSkPrefix(PaperDeliveryCounter.SkPrefix.SUM_ESTIMATES, product.getValue(), province);
                    String shipmentDate = deliveryWeek.minusWeeks(1).toString();
                    return paperDeliveryCounterDAO.getPaperDeliveryCounter(shipmentDate, sk, 1)
                            .flatMapMany(Flux::fromIterable)
                            .map(counter -> Tuples.of(product, counter));
                })
                .collectList()
                .filter(paperDeliveryCountersTuple -> !CollectionUtils.isEmpty(paperDeliveryCountersTuple))
                .doOnNext(paperDeliveryCountersTuple -> paperDeliveryCountersTuple
                        .forEach(tuple -> totalEstimateCounter.put(tuple.getT1().getValue(), tuple.getT2().getNumberOfShipments())))
                .thenReturn(totalEstimateCounter);
    }

    /**
     * Retrieves the used sender limit for the given delivery week and product type tuples.
     * The results are stored in the provided senderLimitMap.
     * @param shipmentDate LocalDate representing the delivery date
     * @param paIdProductTypeTuples Set of tuples representing paId~product
     * @param senderLimitMap Map containing limit for each paId~productType tuple
     * */
    private Mono<Map<String, Tuple2<Integer, Integer>>> retrieveUsedSenderLimit(LocalDate shipmentDate, Set<String> paIdProductTypeTuples, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        return Flux.fromIterable(paIdProductTypeTuples).buffer(25)
                .flatMap(usedSenderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(usedSenderLimitPkSubList, shipmentDate))
                .collectList()
                .doOnNext(paperDeliveryUsedSenderLimits ->
                        paperDeliveryUsedSenderLimits.forEach(paperDeliveryUsedSenderLimit -> senderLimitMap.put(paperDeliveryUsedSenderLimit.getPk(), Tuples.of(paperDeliveryUsedSenderLimit.getSenderLimit(), paperDeliveryUsedSenderLimit.getNumberOfShipment()))))
                .thenReturn(senderLimitMap);
    }

    /**
     * Retrieves the sender limits for the specified delivery date only for the paIdProductType entries
     * that are not already present in the provided senderLimitMap.
     * For each retrieved entry, calculates the limit based on the corresponding drivers' total capacity.
     * The computed limits are then stored in the senderLimitMap.
     * @param shipmentDate The date for which the sender limits are to be retrieved
     * @param driversTotalCapacity List containing calculated capacities for each products or list of products and related unifiedDeliveryDrivers
     * @param paIdProductTypeTuples Set of tuple of paId~productType
     * @param senderLimitJobProcessObjects object containing map for senderLimit and totalEstimateCounter
     */
    private Mono<Map<String, Tuple2<Integer, Integer>>> retrieveAndCalculateSenderLimit(LocalDate shipmentDate, List<DriversTotalCapacity> driversTotalCapacity, Set<String> paIdProductTypeTuples, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        List<String> paIdProductTypeTuplesCopy = new ArrayList<>(paIdProductTypeTuples);
        paIdProductTypeTuplesCopy.removeIf(paIdProductTypeTuple -> senderLimitJobProcessObjects.getSenderLimitMap().containsKey(paIdProductTypeTuple));
        return Flux.fromIterable(paIdProductTypeTuplesCopy).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, shipmentDate)
                        .map(paperDeliverySenderLimit -> Tuples.of(paperDeliverySenderLimit.getPk(), calculateLimit(driversTotalCapacity, senderLimitJobProcessObjects, paperDeliverySenderLimit))))
                .collectList()
                .doOnNext(pkLimitTuples -> pkLimitTuples.forEach(tuple -> senderLimitJobProcessObjects
                        .getSenderLimitMap()
                        .put(tuple.getT1(), Tuples.of(tuple.getT2(), 0))))
                .thenReturn(senderLimitJobProcessObjects.getSenderLimitMap());
    }

    private static Integer calculateLimit(List<DriversTotalCapacity> driversTotalCapacity,
                                          SenderLimitJobProcessObjects senderLimitJobProcessObjects,
                                          PaperDeliverySenderLimit paperDeliverySenderLimit) {

        String productType = paperDeliverySenderLimit.getProductType();

        return driversTotalCapacity.stream()
                .filter(d -> d.getProducts().contains(productType))
                .findFirst()
                .map(driver -> SenderLimitUtils.retrieveCapacityAndCalculateLimit(driver, senderLimitJobProcessObjects, paperDeliverySenderLimit))
                .orElse(0);

    }

    private static Integer retrieveCapacityAndCalculateLimit(DriversTotalCapacity driver, SenderLimitJobProcessObjects senderLimitJobProcessObjects, PaperDeliverySenderLimit paperDeliverySenderLimit) {
        int declaredCapacity = driver.getCapacity();
        List<String> relevantProducts = driver.getProducts().stream()
                .filter(p -> !ProductType.RS.getValue().equalsIgnoreCase(p))
                .toList();

        int totalEstimate;
        if (relevantProducts.size() > 1) {
            totalEstimate = relevantProducts.stream()
                    .filter(product -> senderLimitJobProcessObjects.getTotalEstimateCounter().containsKey(product))
                    .mapToInt(product -> senderLimitJobProcessObjects.getTotalEstimateCounter().get(product))
                    .sum();
        } else {
            totalEstimate = Optional.ofNullable(senderLimitJobProcessObjects.getTotalEstimateCounter().get(paperDeliverySenderLimit.getProductType())).orElse(0);
        }

        if (totalEstimate == 0) {
            return 0;
        }

        double percentage = (double) paperDeliverySenderLimit.getWeeklyEstimate() / totalEstimate;
        int limit = (int) Math.floor(declaredCapacity * percentage);
        log.info("Calculated [{}] as limit for productType: {}, paId: {}, province: {} with declaredProvinceCapacity: {}, totalEstimate: {}, weeklyEstimate: {}",
                limit, paperDeliverySenderLimit.getProductType(), paperDeliverySenderLimit.getPaId(), paperDeliverySenderLimit.getProvince(), declaredCapacity, totalEstimate, paperDeliverySenderLimit.getWeeklyEstimate());
        return limit;
    }

    public Flux<IncrementUsedSenderLimitDto> createIncrementUsedSenderLimitDtos(List<PaperDelivery> paperDeliveryList, Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        Map<String, Long> usedSenderLimitMap = pnDelayerUtils.groupByPaIdProductTypeProvinceAndCount(paperDeliveryList);
        return Flux.fromIterable(usedSenderLimitMap.entrySet())
                .map(tupleCounterEntry -> new IncrementUsedSenderLimitDto(tupleCounterEntry.getKey(), tupleCounterEntry.getValue(), senderLimitMap.get(tupleCounterEntry.getKey()).getT1()));
    }
}

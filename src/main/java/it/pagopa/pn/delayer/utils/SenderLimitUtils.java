package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.exception.InvalidSenderLimitException;
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

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_INVALID_PERCENTAGE;

@Component
@Slf4j
@RequiredArgsConstructor
public class SenderLimitUtils {

    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final PnDelayerUtils pnDelayerUtils;
    private final PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    public Mono<SenderLimitJobProcessObjects> retrieveAndEvaluateSenderLimit(LocalDate deliveryWeek, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        LocalDate shipmentDate = deliveryWeek.minusWeeks(1);
        return retrieveAndCalculateSenderLimit(shipmentDate, driversTotalCapacity, deliveriesGroupedByProductTypePaId.keySet(), senderLimitJobProcessObjects)
                .doOnNext(unused -> pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(senderLimitJobProcessObjects.getSenderLimitMap(), deliveriesGroupedByProductTypePaId, senderLimitJobProcessObjects))
                .thenReturn(senderLimitJobProcessObjects);
    }

    public Mono<Map<String, Integer>> retrieveTotalEstimateCounter(LocalDate deliveryWeek, String province) {
        Map<String, Integer> totalEstimateCounter = new HashMap<>();
        return Flux.fromStream(Arrays.stream(ProductType.values()))
                .flatMap(product -> getSumEstimateCounterWithFallback(deliveryWeek, province, product.getValue())
                        .map(paperDeliveryCounter -> Tuples.of(product, paperDeliveryCounter)))
                .collectList()
                .filter(paperDeliveryCountersTuple -> !CollectionUtils.isEmpty(paperDeliveryCountersTuple))
                .doOnNext(paperDeliveryCountersTuple -> paperDeliveryCountersTuple
                        .forEach(tuple -> totalEstimateCounter.put(tuple.getT1().getValue(), tuple.getT2().getNumberOfShipments())))
                .thenReturn(totalEstimateCounter);
    }

    // metodo che recupera la counter SUM_ESTIMATES con fallback per retro-compatibilità sulla vecchia query
    // a causa del nuovo formato da BEGIN WITH sk = SUM_ESTIMATES~<province>~<product>~
    // a sk EQUALS SUM_ESTIMATES~<province>~<product>
    private Mono<PaperDeliveryCounter> getSumEstimateCounterWithFallback(LocalDate deliveryWeek, String province, String product) {
        String sk = PaperDeliveryCounter.buildSk(PaperDeliveryCounter.SkPrefix.SUM_ESTIMATES, product, province);
        String skPrefix = PaperDeliveryCounter.buildSkPrefix(PaperDeliveryCounter.SkPrefix.SUM_ESTIMATES, product, province);
        String shipmentDate = deliveryWeek.minusWeeks(1).toString();
        return paperDeliveryCounterDAO.getPaperDeliveryCounter(shipmentDate, sk)
                .switchIfEmpty(Mono.defer(() -> paperDeliveryCounterDAO.getPaperDeliveryCounter(shipmentDate, skPrefix, 1)
                        .doOnNext(unused -> log.info("Retrieve counters with fallback: {}", skPrefix))
                        .flatMap(paperDeliveryCounters -> CollectionUtils.isEmpty(paperDeliveryCounters) ? Mono.empty() : Mono.just(paperDeliveryCounters.getFirst()))));
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
        if (percentage > 1) {
            log.error("Invalid sender limit percentage [{}] for productType: {}, paId: {}, province: {} with totalEstimate: {}, weeklyEstimate: {}",
                    percentage, paperDeliverySenderLimit.getProductType(), paperDeliverySenderLimit.getPaId(), paperDeliverySenderLimit.getProvince(), totalEstimate, paperDeliverySenderLimit.getWeeklyEstimate());
            throw new InvalidSenderLimitException(String.format("Sender limit percentage exceeds 100%% for productType=%s, paId=%s, province=%s",
                    paperDeliverySenderLimit.getProductType(), paperDeliverySenderLimit.getPaId(), paperDeliverySenderLimit.getProvince()), ERROR_CODE_INVALID_PERCENTAGE);
        }
        
        int limit = (int) Math.floor(declaredCapacity * percentage);
        log.info("Calculated [{}] as limit for productType: {}, paId: {}, province: {} with declaredProvinceCapacity: {}, totalEstimate: {}, weeklyEstimate: {}",
                limit, paperDeliverySenderLimit.getProductType(), paperDeliverySenderLimit.getPaId(), paperDeliverySenderLimit.getProvince(), declaredCapacity, totalEstimate, paperDeliverySenderLimit.getWeeklyEstimate());
        return limit;
    }

    public Flux<IncrementUsedSenderLimitDto> createIncrementUsedSenderLimitDtos(Map<String, Tuple2<Integer, Integer>> senderLimitMap) {
        return Flux.fromIterable(senderLimitMap.entrySet())
                .filter(calculateAndUsedEntry -> calculateAndUsedEntry.getValue().getT2() > 0)
                .map(calculateAndUsedEntry -> new IncrementUsedSenderLimitDto(calculateAndUsedEntry.getKey(), calculateAndUsedEntry.getValue().getT2(), calculateAndUsedEntry.getValue().getT1()));
    }
}

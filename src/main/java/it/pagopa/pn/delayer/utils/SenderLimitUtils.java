package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.exception.InvalidSenderLimitException;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
import it.pagopa.pn.delayer.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
                .doOnNext(unused -> pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(senderLimitJobProcessObjects.getSenderLimitMap(), deliveriesGroupedByProductTypePaId, senderLimitJobProcessObjects, deliveryWeek))
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

    public Mono<List<PaperDeliveryCounter>> retrieveDelayedCounters(LocalDate deliveryWeek, String province) {
        return paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryWeek.toString(), PaperDeliveryCounter.SkPrefix.DELAYED.getValue() + province, null)
                .defaultIfEmpty(List.of());
    }

    public Mono<Map<String, Integer>> getResidualCapacity(SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        List<PaperDeliveryCounter> counters = senderLimitJobProcessObjects.getDelayedCounters();
        if (CollectionUtils.isEmpty(counters)) {
            return Mono.just(Map.of());
        }

        return Flux.fromIterable(counters)
                .collectMultimap(PaperDeliveryCounter::getNotificationSentAtWeek)
                .flatMapMany(groupedCounters -> Flux.fromIterable(groupedCounters.entrySet()))
                .flatMap(entry -> buildResidualCapacity(entry.getKey(), List.copyOf(entry.getValue()), senderLimitJobProcessObjects))
                .reduce(new HashMap<>(), (result, partialResult) -> {
                    result.putAll(partialResult);
                    return result;
                });
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
     * Retrieves the sender limits for the specified shipment date for the
     * {@code paId~productType} entries that are not already present in the sender limit map.
     *
     * <p>Entries in the map use the following key format:</p>
     *
     * <pre>
     * shipmentDate~paId~productType
     * </pre>
     *
     * <p>For each retrieved sender limit, the method calculates the guaranteed limit
     * based on the corresponding drivers' total capacity and stores a
     * {@link SenderLimitData} containing:</p>
     *
     * <ul>
     *   <li>the configured weekly estimate;</li>
     *   <li>the calculated guaranteed limit;</li>
     *   <li>the used limit, initialized to {@code 0};</li>
     *   <li>the shipment date associated with the limit.</li>
     * </ul>
     *
     * @param shipmentDate the shipment date for which the sender limits are retrieved
     * @param driversTotalCapacity the calculated capacities for each product or product group
     *                             and the related unified delivery drivers
     * @param paIdProductTypeTuples the set of keys in {@code paId~productType} format
     * @param senderLimitJobProcessObjects the object containing the sender limit map
     *                                     and the total estimate counter
     * @return a {@link Mono} emitting the updated sender limit map
     */
    private Mono<Map<String, SenderLimitData>> retrieveAndCalculateSenderLimit(LocalDate shipmentDate, List<DriversTotalCapacity> driversTotalCapacity, Set<String> paIdProductTypeTuples, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        List<String> paIdProductTypeTuplesCopy = new ArrayList<>(paIdProductTypeTuples);
        paIdProductTypeTuplesCopy.removeIf(paIdProductTypeTuple -> {
            String senderLimitMapKey = String.join("~", shipmentDate.toString(), paIdProductTypeTuple);
            return senderLimitJobProcessObjects.getSenderLimitMap().containsKey(senderLimitMapKey);
        });

        return Flux.fromIterable(paIdProductTypeTuplesCopy).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, shipmentDate)
                        .map(paperDeliverySenderLimit -> {
                            int calculatedLimit = calculateLimit(driversTotalCapacity, senderLimitJobProcessObjects, paperDeliverySenderLimit);
                            return Tuples.of(paperDeliverySenderLimit.getPk(), SenderLimitData.initial(paperDeliverySenderLimit.getWeeklyEstimate(),
                                    calculatedLimit, shipmentDate));
                        }))
                .collectList()
                .doOnNext(pkLimitTuples -> pkLimitTuples.forEach(tuple -> {
                    String senderLimitMapKey = String.join("~", shipmentDate.toString(), tuple.getT1());
                    senderLimitJobProcessObjects.getSenderLimitMap().put(senderLimitMapKey, tuple.getT2());
                }))
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
            log.info("Total estimate is zero for productType: {}, paId: {}, province: {}. Returning limit as 0.", paperDeliverySenderLimit.getProductType(), paperDeliverySenderLimit.getPaId(), paperDeliverySenderLimit.getProvince());
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

    public Flux<IncrementUsedSenderLimitDto> createIncrementUsedSenderLimitDtos(Map<String, SenderLimitData> senderLimitMap) {
        return Flux.fromIterable(senderLimitMap.entrySet())
                .filter(entry -> entry.getValue().usedLimit() > 0)
                .map(entry -> {
                    String[] keyParts = entry.getKey().split("~", 2);
                    String pk = keyParts[1];
                    return new IncrementUsedSenderLimitDto(pk, entry.getValue().usedLimit(), entry.getValue().calculatedLimit(), entry.getValue().date());
                });
    }

    private Mono<Map<String, Integer>> buildResidualCapacity(String notificationSentAtWeek, List<PaperDeliveryCounter> counters, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        if (CollectionUtils.isEmpty(counters)) {
            return Mono.just(Map.of());
        }
        LocalDate sentAtWeek = LocalDate.parse(notificationSentAtWeek);
        List<String> senderLimitKeys = counters.stream()
                .map(PaperDeliveryCounter::getSk)
                .map(this::extractSk)
                .distinct()
                .toList();

        return paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(senderLimitKeys, sentAtWeek)
                .collectMap(PaperDeliveryUsedSenderLimit::getPk, item -> Optional.ofNullable(item.getNumberOfShipment()).orElse(0))
                .map(usedCapacityBySender -> {
                    Map<String, Integer> residualCapacityBySender = new HashMap<>();
                    for (PaperDeliveryCounter counter : counters) {
                        String senderLimitKey = extractSk(counter.getSk());
                        int weeklyEstimate = Optional.ofNullable(counter.getWeeklyEstimate()).orElse(0);
                        int usedCapacity = usedCapacityBySender.getOrDefault(senderLimitKey, 0);
                        String resultKey = String.join("~", notificationSentAtWeek, senderLimitKey);
                        senderLimitJobProcessObjects.getSenderLimitMap().put(resultKey, new SenderLimitData(weeklyEstimate, null, usedCapacity, sentAtWeek));
                    }
                    return residualCapacityBySender;
                })
                .defaultIfEmpty(new HashMap<>());
    }

    private String extractSk(String sk) {
        String[] parts = sk.split("~", -1);
        if (parts.length < 4) {
            throw new PnInternalException("INVALID USEDSENDERLIMIT_SK", "Invalid PaperDeliveryCounter sk: " + sk);
        }
        String province = parts[1];
        String productType = parts[2];
        String senderPaId = parts[3];
        return String.join("~", senderPaId, productType, province);
    }
}

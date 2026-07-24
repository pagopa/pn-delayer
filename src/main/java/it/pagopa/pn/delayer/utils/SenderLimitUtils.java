package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.exception.InvalidSenderLimitException;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
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

    public Mono<List<PaperDeliveryCounter>> retrieveDelayedCounters(LocalDate deliveryWeek, String province) {
        return paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryWeek.toString(), PaperDeliveryCounter.SkPrefix.DELAYED.name() + province, null)
                .defaultIfEmpty(List.of());
    }

    public Mono<Map<String, Integer>> getResidualCapacity(List<PaperDeliveryCounter> counters) {
        if (CollectionUtils.isEmpty(counters)) {
            return Mono.just(Map.of());
        }

        return Flux.fromIterable(counters)
                .collectMultimap(PaperDeliveryCounter::getNotificationSentAtWeek)
                .flatMapMany(groupedCounters -> Flux.fromIterable(groupedCounters.entrySet()))
                .flatMap(entry ->
                        buildResidualCapacity(entry.getKey(), List.copyOf(entry.getValue()))
                )
                .reduce(new HashMap<String, Integer>(), (result, partialResult) -> {
                    result.putAll(partialResult);
                    return result;
                })
                .map(Map::copyOf);
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
     * Retrieves the sender limits for the specified shipment date only for the
     * paId~productType entries that are not already present in the senderLimitMap.
     * For each retrieved sender limit, calculates the guaranteed limit based on the
     * corresponding drivers' total capacity and stores the following information
     * in the senderLimitMap:
     * <ul>
     *   <li>the configured weekly estimate;</li>
     *   <li>the calculated guaranteed limit;</li>
     *   <li>the used limit, initialized to {@code 0}.</li>
     * </ul>
     *
     * @param shipmentDate the shipment date for which sender limits are retrieved
     * @param driversTotalCapacity the calculated capacities for each product or product group
     *                             and the related unified delivery drivers
     * @param paIdProductTypeTuples the set of {@code paId~productType} keys
     * @param senderLimitJobProcessObjects the object containing the sender limit map
     *                                     and the total estimate counter
     * @return a {@link Mono} containing the updated sender limit map
     */
    private Mono<Map<String, SenderLimitJobProcessObjects.SenderLimitData>> retrieveAndCalculateSenderLimit(LocalDate shipmentDate, List<DriversTotalCapacity> driversTotalCapacity, Set<String> paIdProductTypeTuples, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {

        List<String> missingSenderLimits = paIdProductTypeTuples.stream()
                .filter(pk -> !senderLimitJobProcessObjects.getSenderLimitMap().containsKey(pk))
                .toList();

        return Flux.fromIterable(missingSenderLimits)
                .buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO
                        .retrieveSendersLimit(senderLimitPkSubList, shipmentDate))
                .map(paperDeliverySenderLimit -> {
                    Integer calculatedLimit = calculateLimit(driversTotalCapacity, senderLimitJobProcessObjects, paperDeliverySenderLimit);
                    SenderLimitJobProcessObjects.SenderLimitData senderLimitData = SenderLimitJobProcessObjects.SenderLimitData.initial(paperDeliverySenderLimit.getWeeklyEstimate(), calculatedLimit);
                    return Tuples.of(paperDeliverySenderLimit.getPk(), senderLimitData);
                })
                .collectMap(Tuple2::getT1, Tuple2::getT2)
                .doOnNext(senderLimits -> senderLimitJobProcessObjects.getSenderLimitMap().putAll(senderLimits))
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

    public Flux<IncrementUsedSenderLimitDto> createIncrementUsedSenderLimitDtos(Map<String, SenderLimitJobProcessObjects.SenderLimitData> senderLimitMap) {
        return Flux.fromIterable(senderLimitMap.entrySet())
                .filter(entry -> entry.getValue().usedLimit() > 0)
                .map(entry -> new IncrementUsedSenderLimitDto(entry.getKey(), entry.getValue().usedLimit(), entry.getValue().calculatedLimit()));
    }

    private Mono<Map<String, Integer>> buildResidualCapacity(String notificationSentAtWeek, List<PaperDeliveryCounter> counters) {
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
                        int residualCapacity = Math.max(weeklyEstimate - usedCapacity, 0);
                        String resultKey = notificationSentAtWeek + "~" + senderLimitKey;
                        residualCapacityBySender.put(resultKey, residualCapacity);
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

    public void adjustDriverCapacitiesForSkipSenderLimitDeliveries(Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
    }
}

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
                .doOnNext(_ -> pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(senderLimitJobProcessObjects.getSenderLimitMap(), deliveriesGroupedByProductTypePaId, senderLimitJobProcessObjects, deliveryWeek))
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
        return paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryWeek.toString(), PaperDeliveryCounter.buildSk(PaperDeliveryCounter.SkPrefix.DELAYED,province), null)
                .defaultIfEmpty(List.of());
    }

    /**
     * Popola la mappa dei SenderLimitData per le spedizioni ritardate dal sistema,
     * a partire dai contatori di tali spedizioni.
     * <p>
     * Le spedizioni vengono raggruppate in base alla settimana di spedizione
     * originaria e, per ciascun gruppo, viene recuperata dal datastore la quota
     * di sender limit già utilizzata. Tale valore costituisce la baseline dalla
     * quale prosegue la valutazione del sender limit durante l'elaborazione
     * corrente.
     *
     * @param senderLimitJobProcessObjects oggetti condivisi utilizzati durante il job
     * @return oggetti di processo aggiornati con la capacità residua delle
     * spedizioni ritardate
     */
    public Mono<SenderLimitJobProcessObjects> getResidualCapacityForDelayed(SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        List<PaperDeliveryCounter> counters = senderLimitJobProcessObjects.getDelayedCounters();
        if (CollectionUtils.isEmpty(counters)) {
            return Mono.just(senderLimitJobProcessObjects);
        }

        return Flux.fromIterable(counters)
                .collectMultimap(PaperDeliveryCounter::getNotificationSentAtWeek)
                .flatMapMany(groupedCounters -> Flux.fromIterable(groupedCounters.entrySet()))
                .concatMap(entry -> buildResidualCapacity(entry.getKey(), List.copyOf(entry.getValue()), senderLimitJobProcessObjects))
                .then(Mono.just(senderLimitJobProcessObjects));
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
                        .doOnNext(_ -> log.info("Retrieve counters with fallback: {}", skPrefix))
                        .flatMap(paperDeliveryCounters -> CollectionUtils.isEmpty(paperDeliveryCounters) ? Mono.empty() : Mono.just(paperDeliveryCounters.getFirst()))));
    }

    /**
     * Recupera e calcola il sender limit per tutti i mittenti non ancora presenti
     * nella mappa condivisa del job.
     * <p>
     * Per ciascun mittente viene recuperata la configurazione persistita, viene
     * calcolato il limite garantito in funzione della capacità disponibile dei
     * driver e viene creata una nuova istanza di {@link SenderLimitData}.
     * <p>
     * I sender già presenti nella mappa vengono ignorati. Questo comportamento è
     * necessario per evitare di ricalcolare il sender limit dei mittenti già
     * inizializzati durante la gestione delle spedizioni ritardate.
     *
     * @param shipmentDate settimana di spedizione di riferimento
     * @param driversTotalCapacity capacità dichiarata dei driver
     * @param paIdProductTypeTuples insieme delle chiavi nel formato
     *                              {@code paId~productType~province}
     * @param senderLimitJobProcessObjects oggetti condivisi utilizzati durante il job
     * @return mappa aggiornata contenente il sender limit di tutti i mittenti coinvolti
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
                .filter(entry -> entry.getValue().incrementUsedLimit() > 0)
                .map(entry -> {
                    String[] keyParts = entry.getKey().split("~", 2);
                    SenderLimitData data = entry.getValue();

                    return new IncrementUsedSenderLimitDto(
                            keyParts[1],
                            data.incrementUsedLimit(),
                            data.calculatedLimit(),
                            data.date()
                    );
                });
    }

    /**
     * Inizializza le informazioni relative al modulo commessa e alla quota mittente già utilizzata
     * per le notifiche ritardate dal sistema per una specifica settimana di riferimento.
     * <p>
     * Per ciascun mittente vengono recuperate:
     * <ul>
     *     <li>la stima settimanale dal contatore delle spedizioni ritardate;</li>
     *     <li>la quota mittente già utilizzata nella settimana di riferimento,
     *     recuperata dal datastore.</li>
     * </ul>
     * La quota già utilizzata costituisce la baseline da cui ripartire durante la
     * valutazione corrente, evitando di riconsiderare come disponibili quote di
     * sender limit già consumate in precedenti esecuzioni dell'algoritmo.
     * <p>
     * Se per un mittente non è presente alcun record persistito, la baseline viene
     * inizializzata a {@code 0}.
     *
     * @param notificationSentAtWeek settimana di spedizione originaria
     * @param counters contatori delle spedizioni ritardate appartenenti alla settimana
     * @param senderLimitJobProcessObjects oggetti condivisi utilizzati durante il job
     * @return completamento dell'inizializzazione delle informazioni di sender limit
     */
    private Mono<Void> buildResidualCapacity(String notificationSentAtWeek, List<PaperDeliveryCounter> counters, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        if (CollectionUtils.isEmpty(counters)) {
            return Mono.empty();
        }
        LocalDate sentAtWeek = LocalDate.parse(notificationSentAtWeek);
        List<String> senderLimitKeys = counters.stream()
                .map(PaperDeliveryCounter::getSk)
                .map(this::extractSk)
                .distinct()
                .toList();

        return paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(senderLimitKeys, sentAtWeek)
                .collectMap(PaperDeliveryUsedSenderLimit::getPk, item -> Optional.ofNullable(item.getNumberOfShipment()).orElse(0))
                .doOnNext(usedCapacityBySender -> {
                    for (PaperDeliveryCounter counter : counters) {
                        String senderLimitKey = extractSk(counter.getSk());
                        int weeklyEstimate = Objects.requireNonNullElse(counter.getWeeklyEstimate(), 0);
                        int baselineUsedLimit = usedCapacityBySender.getOrDefault(senderLimitKey, 0);
                        String resultKey = String.join("~", notificationSentAtWeek, senderLimitKey);
                        senderLimitJobProcessObjects.getSenderLimitMap().put(resultKey,
                                SenderLimitData.initialWithBaseline(
                                        weeklyEstimate,
                                        baselineUsedLimit,
                                        sentAtWeek
                                ));
                    }
                })
                .then();
    }

    private String extractSk(String sk) {
        String[] parts = sk.split("~", -1);
        if (parts.length < 4) {
            throw new PnInternalException("Invalid PaperDeliveryCounter sk: " + sk,"INVALID USEDSENDERLIMIT_SK");
        }
        String province = parts[1];
        String productType = parts[2];
        String senderPaId = parts[3];
        return String.join("~", senderPaId, productType, province);
    }
}

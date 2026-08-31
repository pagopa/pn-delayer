package it.pagopa.pn.delayer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
import it.pagopa.pn.delayer.utils.DeliveryDriverUtils;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import it.pagopa.pn.delayer.utils.PnDelayerUtils;
import it.pagopa.pn.delayer.utils.SenderLimitUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.PAPER_DELIVERY_PRIORITY_MAP_ERROR;
import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.PAPER_DELIVERY_PRIORITY_MAP_NOT_FOUND;

@Component
@Slf4j
@RequiredArgsConstructor
public class EvaluateSenderLimitJobServiceImpl implements EvaluateSenderLimitJobService {

    private final PnDelayerUtils pnDelayerUtils;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final PaperDeliveryUtils paperDeliveryUtils;
    private final DeliveryDriverUtils deliveryDriverUtils;
    private final SsmParameterConsumerActivation ssmParameterConsumerActivation;
    private final SenderLimitUtils senderLimitUtils;
    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final ObjectMapper objectMapper;


    /**
     * Avvia il processo di valutazione del sender limit per una provincia e una
     * settimana di recapito.
     * <p>
     * Il flusso di elaborazione è composto dalle seguenti fasi:
     * <ol>
     *     <li>inizializzazione degli oggetti condivisi utilizzati durante il job;</li>
     *     <li>recupero dei contatori delle stime settimanali per i mittenti;</li>
     *     <li>recupero della capacità dei recapitisti per la provincia;</li>
     *     <li>lettura paginata delle spedizioni da elaborare e valutazione del
     *     limite garantito;</li>
     *     <li>persistenza dell'incremento della quota mittente utilizzata durante
     *     l'elaborazione.</li>
     * </ol>
     *
     * @param province provincia oggetto dell'elaborazione
     * @param tenderId gara di riferimento
     * @param deliveryWeek settimana di recapito
     * @return completamento del job
     */
    @Override
    public Mono<Void> startSenderLimitJob(String province, String tenderId, LocalDate deliveryWeek){
        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();
        senderLimitJobProcessObjects.setPriorityMap(getPriorityMap());
        return senderLimitUtils.retrieveTotalEstimateCounter(deliveryWeek, province)
                .doOnNext(senderLimitJobProcessObjects::setTotalEstimateCounter)
                .flatMap(_ -> deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .flatMap(driversTotalCapacities -> retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, new HashMap<>(), driversTotalCapacities, senderLimitJobProcessObjects))
                .flatMap(processObj -> flushCounters(processObj.getSenderLimitMap()))
                .doOnError(error -> log.error("Error processing sender limit job for province: {}, tenderId: {}, deliveryWeek: {}", province, tenderId, deliveryWeek, error));
    }

    private Mono<SenderLimitJobProcessObjects> retrieveAndProcessPaperDeliveries(String province, String tenderId, LocalDate deliveryWeek, Map<String, AttributeValue> lastEvaluatedKey, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        var sortkeyPrefix = province + "~";
        return paperDeliveryUtils.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, deliveryWeek, sortkeyPrefix, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit())
                .flatMap(paperDeliveryPage -> processItems(paperDeliveryPage.items(), tenderId, deliveryWeek, driversTotalCapacity, senderLimitJobProcessObjects)
                        .flatMap(processObjects -> {
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey())) {
                                log.info("Processed items for province: {}, tenderId: {}, deliveryWeek: {}. Continuing with lastEvaluatedKey: {}", province, tenderId, deliveryWeek, paperDeliveryPage.lastEvaluatedKey());
                                processObjects.getSendToDriverCapacityStep().clear();
                                processObjects.getSendToResidualCapacityStep().clear();
                                return retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, paperDeliveryPage.lastEvaluatedKey(), driversTotalCapacity, processObjects);
                            }
                            log.info("Processed items for province: {}, tenderId: {}, deliveryWeek: {}. No more items to process.", province, tenderId, deliveryWeek);
                            return Mono.just(processObjects);
                        }));
    }

    private Mono<Void> flushCounters(Map<String, SenderLimitData> senderLimitMap) {
        return senderLimitUtils.createIncrementUsedSenderLimitDtos(senderLimitMap)
                .collectList()
                .filter(incrementUsedSenderLimitDtoList -> !CollectionUtils.isEmpty(incrementUsedSenderLimitDtoList))
                .map(incrementUsedSenderLimitDtoList -> incrementUsedSenderLimitDtoList.stream()
                        .collect(Collectors.groupingBy(incrementUsedSenderLimitDto -> String.join("#", incrementUsedSenderLimitDto.pk(), incrementUsedSenderLimitDto.senderLimit().toString(), incrementUsedSenderLimitDto.weeklyEstimate().toString(), incrementUsedSenderLimitDto.shipmentDate().toString()),
                                Collectors.summingLong(dto -> dto.increment() == null ? 0 : dto.increment())
                        )))
                .map(Map::entrySet)
                .flatMapIterable(entries -> entries)
                .flatMap(entry -> {
                    String[] keyParts = entry.getKey().split("#", 4);
                    String pk = keyParts[0];
                    Integer senderLimit = Integer.valueOf(keyParts[1]);
                    Integer weeklyEstimate = Integer.valueOf(keyParts[2]);
                    LocalDate shipmentDate = LocalDate.parse(keyParts[3]);
                    return paperDeliverySenderLimitDAO.updateUsedSenderLimit(pk, entry.getValue(), shipmentDate, senderLimit, weeklyEstimate);
                })
                .then();
    }

    /**
     * Elabora l'elenco delle spedizioni cartacee eseguendo le seguenti operazioni:
     * <ol>
     *     <li>recupera il driver unificato di recapito e lo assegna a ciascuna spedizione;</li>
     *     <li>esclude dalla valutazione del sender limit le spedizioni di tipo RS, i secondi tentativi e
     *     i primi tentativi con skipSenderLimit == true,
     *     inoltrandole direttamente allo step {@code EVALUATE_DRIVER_CAPACITY};</li>
     *     <li>raggruppa le restanti spedizioni per tipologia di prodotto, mittente e provincia;</li>
     *     <li>calcola e valuta il sender limit per ciascun gruppo, classificando le spedizioni
     *     tra gli step {@code EVALUATE_DRIVER_CAPACITY} ed {@code EVALUATE_RESIDUAL_CAPACITY};</li>
     *     <li>persiste le nuove entità nella tabella {@code PaperDelivery}, associate al
     *     rispettivo step di elaborazione;</li>
     *     <li>aggiorna i contatori del sender limit utilizzato per le spedizioni destinate
     *     allo step {@code EVALUATE_DRIVER_CAPACITY}.</li>
     * </ol>
     *
     * @param items elenco delle spedizioni da elaborare
     * @param tenderId identificativo della gara associata alle spedizioni
     * @param deliveryWeek settimana di recapito oggetto dell'elaborazione
     * @param driversTotalCapacity capacità dei driver di recapito per la provincia
     * @param senderLimitJobProcessObjects oggetti condivisi utilizzati durante l'elaborazione del job
     * @return gli oggetti di processo aggiornati al termine dell'elaborazione
     */
    private Mono<SenderLimitJobProcessObjects> processItems(List<PaperDelivery> items, String tenderId, LocalDate deliveryWeek, List<DriversTotalCapacity> driversTotalCapacity, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        return retrieveUnifiedDeliveryDriverAndAssignToPaperDeliveries(items, tenderId, driversTotalCapacity, senderLimitJobProcessObjects.getPriorityMap())
                .map(paperDeliveries -> pnDelayerUtils.classifyAndGroupForSenderLimit(paperDeliveries, senderLimitJobProcessObjects))
                .flatMap(deliveriesGroupedByProductTypePaId -> senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, driversTotalCapacity, senderLimitJobProcessObjects))
                .flatMap(deliveries -> paperDeliveryUtils.insertPaperDeliveries(deliveries, deliveryWeek))
                .filter(sentToNextStep -> !CollectionUtils.isEmpty(sentToNextStep))
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No items to send to evaluate driver capacity step for tenderId: {}, deliveryWeek: {}", tenderId, deliveryWeek);
                    return Mono.empty();
                }))
                .thenReturn(senderLimitJobProcessObjects);
    }

    /**
     * Recupera e assegna il driver unificato di recapito a ciascuna spedizione.
     * <p>
     * Se per la provincia è disponibile un solo driver unificato, questo viene
     * assegnato direttamente a tutte le spedizioni senza effettuare ulteriori
     * interrogazioni.
     * <p>
     * In caso di più driver disponibili, il metodo tenta innanzitutto di recuperare
     * le associazioni CAP/prodotto - driver dalla cache. Per le sole associazioni
     * non presenti in cache viene invocato Paper Channel, che restituisce il driver
     * unificato da assegnare alle spedizioni. Le associazioni recuperate vengono
     * quindi salvate in cache per le elaborazioni successive.
     *
     * @param paperDelivery elenco delle spedizioni da arricchire
     * @param tenderId identificativo della gara
     * @param driversTotalCapacity capacità dei driver disponibili per la provincia
     * @param priorityMap mappa delle priorità utilizzata per valorizzare le spedizioni
     * @return elenco delle spedizioni arricchite con driver unificato e priorità
     */
    private Mono<List<PaperDelivery>> retrieveUnifiedDeliveryDriverAndAssignToPaperDeliveries(List<PaperDelivery> paperDelivery, String tenderId, List<DriversTotalCapacity> driversTotalCapacity, Map<Integer, List<PaperDeliveryPriority>> priorityMap) {
        if (driversTotalCapacity.size() == 1 && driversTotalCapacity.getFirst().getUnifiedDeliveryDrivers().size() == 1) {
            String unifiedDeliveryDriver = driversTotalCapacity.getFirst().getUnifiedDeliveryDrivers().getFirst();
            return Mono.just(deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(paperDelivery, unifiedDeliveryDriver, tenderId, priorityMap));
        } else {
            Map<String, List<PaperDelivery>> groupedByCapProductType = pnDelayerUtils.groupByCapAndProductType(paperDelivery);
            log.info("Number of CAP and Product Type groups to process for tenderId {}: {}", tenderId, groupedByCapProductType.size());
            ConcurrentHashMap<String, List<PaperDelivery>> groupedByCapProductTypeNotInCache = new ConcurrentHashMap<>();
            return Flux.fromIterable(groupedByCapProductType.entrySet())
                    .map(capProductTypeEntry -> retrieveFromCacheAndEnrichPaperDelivery(tenderId, priorityMap, capProductTypeEntry, groupedByCapProductTypeNotInCache))
                    .flatMapIterable(list -> list)
                    .collectList()
                    .flatMap(toSenderLimitEvaluationTmp -> callPaperChannelAndRetrieveEnrichedPaperDelivery(tenderId, priorityMap, groupedByCapProductTypeNotInCache)
                            .map(toSenderLimitEvaluationFromPaperChannel -> {
                                List<PaperDelivery> result = new ArrayList<>(toSenderLimitEvaluationTmp);
                                result.addAll(toSenderLimitEvaluationFromPaperChannel);
                                return result;
                            }));
        }
    }

    private List<PaperDelivery> retrieveFromCacheAndEnrichPaperDelivery(String tenderId, Map<Integer, List<PaperDeliveryPriority>> priorityMap, Map.Entry<String, List<PaperDelivery>> capProductTypeEntry, Map<String, List<PaperDelivery>> groupedByCapProductTypeNotInCache) {
        return deliveryDriverUtils.retrieveFromCache(capProductTypeEntry.getKey())
                .map(unifiedDeliveryDriver -> deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(capProductTypeEntry.getValue(), unifiedDeliveryDriver, tenderId, priorityMap))
                .orElseGet(() -> {
                    groupedByCapProductTypeNotInCache.put(capProductTypeEntry.getKey(), capProductTypeEntry.getValue());
                    return new ArrayList<>();
                });
    }

    private Mono<List<PaperDelivery>> callPaperChannelAndRetrieveEnrichedPaperDelivery(String tenderId, Map<Integer, List<PaperDeliveryPriority>> priorityMap, Map<String, List<PaperDelivery>> groupedByCapProductTypeNotInCache) {

        return Flux.fromIterable(groupedByCapProductTypeNotInCache.keySet())
                .map(capProductTypeKey -> new DeliveryDriverRequest(capProductTypeKey.split("~")[0], capProductTypeKey.split("~")[1]))
                .collectList()
                .doOnNext(requests -> log.info("Number of driver requests for paper channel for tenderId {}: {}", tenderId, requests.size()))
                .filter(requests -> !CollectionUtils.isEmpty(requests))
                .map(requests -> deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(requests, tenderId))
                .doOnNext(deliveryDriverUtils::insertInCache)
                .map(_ -> deliveryDriverUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(groupedByCapProductTypeNotInCache, tenderId, priorityMap))
                .defaultIfEmpty(List.of());
    }

    private Map<Integer, List<PaperDeliveryPriority>> getPriorityMap() {
        String parameterValue = ssmParameterConsumerActivation.getParameter(pnDelayerConfigs.getPaperDeliveryPriorityParameterName());
        if(StringUtils.hasText(parameterValue)){
            try {
                return objectMapper.readValue(parameterValue, new TypeReference<>() {});
            } catch (JsonProcessingException e) {
                log.error("Error parsing paper delivery priority map from SSM parameter store for parameter name: {}", pnDelayerConfigs.getPaperDeliveryPriorityParameterName(), e);
                throw new PnInternalException("Failed to retrieve paper delivery priority map from SSM parameter store", PAPER_DELIVERY_PRIORITY_MAP_ERROR);
            }
        }
        log.error("Priority parameter not found on parameter store for parameter name: {}", pnDelayerConfigs.getPaperDeliveryPriorityParameterName());
        throw new PnInternalException("Priority parameter not found on parameter store", PAPER_DELIVERY_PRIORITY_MAP_NOT_FOUND);
    }
}

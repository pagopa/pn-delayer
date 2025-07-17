package it.pagopa.pn.delayer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class EvaluateSenderLimitJobServiceImpl extends PaperDeliveryService implements EvaluateSenderLimitJobService {

    private final PaperDeliveryUtils paperDeliveryUtils;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final DeliveryDriverCapacityService deliveryDriverCapacityService;
    private final PaperDeliveryDAO paperDeliveryDAO;

    public EvaluateSenderLimitJobServiceImpl(PaperDeliveryDAO paperDeliveryDAO, PnDelayerConfigs pnDelayerConfigs, PaperDeliveryUtils paperDeliveryUtils, DeliveryDriverCapacityService deliveryDriverCapacityService, PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO) {
        super(paperDeliveryDAO, pnDelayerConfigs, paperDeliveryUtils, deliveryDriverCapacityService);
        this.paperDeliveryUtils = paperDeliveryUtils;
        this.pnDelayerConfigs = pnDelayerConfigs;
        this.paperDeliverySenderLimitDAO = paperDeliverySenderLimitDAO;
        this.deliveryDriverCapacityService = deliveryDriverCapacityService;
        this.paperDeliveryDAO = paperDeliveryDAO;
    }
    private final PnDelayerConfigs pnDelayerConfigs;
    private final CapProductTypeDriverCacheService cacheService;
    private final PaperDeliveryUtils paperDeliveryUtils;
    private final LambdaClient lambdaClient;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> startSenderLimitJob(String province, String tenderId, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch) {
        LocalDate deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);
        return deliveryDriverCapacityService.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province)
                .flatMap(driversTotalCapacity -> retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, lastEvaluatedKey, driversTotalCapacity))
                .doOnError(error -> log.error("Error processing sender limit job for province: {}, tenderId: {}, deliveryWeek: {}", province, tenderId, deliveryWeek, error));

    }

    private Mono<Void> retrieveAndProcessPaperDeliveries(String province, String tenderId, LocalDate deliveryWeek, Map<String, AttributeValue> lastEvaluatedKey, DriversTotalCapacity driversTotalCapacity) {
        return retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, deliveryWeek, province, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit())
                .flatMap(paperDeliveryPage -> processItems(paperDeliveryPage.items(), province, tenderId, deliveryWeek, driversTotalCapacity)
                        .flatMap(sentToNextStep -> {
                            if (!CollectionUtils.isEmpty(paperDeliveryPage.lastEvaluatedKey())) {
                                return retrieveAndProcessPaperDeliveries(province, tenderId, deliveryWeek, paperDeliveryPage.lastEvaluatedKey(), driversTotalCapacity);
                            }
                            return Mono.empty();
                        }))
                .then();
    }

    private Mono<Integer> processItems(List<PaperDelivery> items, String province, String tenderId, LocalDate deliveryWeek, DriversTotalCapacity driversTotalCapacity) {
        List<PaperDelivery> sendToResidualStep = new ArrayList<>();
        List<PaperDelivery> sendToNextStep = new ArrayList<>(items.stream().filter(paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS")
                || paperDelivery.getAttempt() == 1).toList());
        items.removeIf(paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS")
                || paperDelivery.getAttempt() == 1);
        SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries = new SenderLimitJobPaperDeliveries();
        senderLimitJobPaperDeliveries.setSendToResidualStep(sendToResidualStep);
        senderLimitJobPaperDeliveries.setSendToNextStep(sendToNextStep);

        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId = paperDeliveryUtils.groupPaperDeliveriesByProductTypePaId(items);
        Map<String, Integer> senderLimitMap = new HashMap<>();
        return retrieveResidualSenderLimit(deliveryWeek, province, deliveriesGroupedByProductTypePaId.keySet(), senderLimitMap)
                .flatMap(residualSenderLimitMap -> calculateSenderLimit(deliveryWeek, driversTotalCapacity.getCapacity(), province, deliveriesGroupedByProductTypePaId.keySet(), residualSenderLimitMap, senderLimitMap))
                .flatMap(residualSenderLimitMap -> evaluateDeliveries(deliveryWeek, residualSenderLimitMap, deliveriesGroupedByProductTypePaId, senderLimitJobPaperDeliveries))
                .flatMap(senderLimitJobPaperDeliveriesObj -> retrieveUnifiedDeliveryDriverAndMapNewEntity(senderLimitJobPaperDeliveriesObj, tenderId, province, deliveryWeek))
                .flatMap(this::insertPaperDeliveries)
                .flatMap(paperDeliveryList -> updateUsedSenderLimit(paperDeliveryList, deliveryWeek, senderLimitMap));
    }

    private Mono<Map<String, Integer>> retrieveResidualSenderLimit(LocalDate deliveryWeek, String province, Set<String> paIdProductTypeTuples, Map<String, Integer> senderLimitMap) {
        return Mono.just(new HashMap<>());
    }

    private Mono<SenderLimitJobPaperDeliveries> retrieveUnifiedDeliveryDriverAndMapNewEntity(SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries, String tenderId, String province, LocalDate deliveryWeek) {
        return Mono.just(senderLimitJobPaperDeliveries);
    }

    private Mono<Integer> updateUsedSenderLimit(List<PaperDelivery> paperDeliveryList, LocalDate deliveryDate, Map<String, Integer> senderLimitMap) {
        Map<String, Long> usedSenderLimitMap = paperDeliveryList.stream()
                .collect(Collectors.groupingBy(
                        paperDelivery -> paperDelivery.getSenderPaId() + "~" + paperDelivery.getProductType() + "~" + paperDelivery.getProvince(),
                        Collectors.counting()));
        return Flux.fromIterable(usedSenderLimitMap.entrySet())
                .flatMap(tupleCounterEntry -> paperDeliverySenderLimitDAO.updateUsedSenderLimit(tupleCounterEntry.getKey(), tupleCounterEntry.getValue().intValue(), deliveryDate, senderLimitMap.get(tupleCounterEntry.getKey())))
                .reduce(0, Integer::sum);
    }

    private Mono<List<PaperDelivery>> insertPaperDeliveries(SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        return paperDeliveryDAO.insertPaperDeliveries(senderLimitJobPaperDeliveries.getSendToNextStep())
                .thenReturn(senderLimitJobPaperDeliveries)
                .flatMap(unused -> paperDeliveryDAO.insertPaperDeliveries(senderLimitJobPaperDeliveries.getSendToResidualStep()))
                .thenReturn(senderLimitJobPaperDeliveries.getSendToNextStep());

    }

    private Mono<SenderLimitJobPaperDeliveries> evaluateDeliveries(LocalDate deliveryWeek, Map<String, Integer> senderLimitMap, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        return Flux.fromIterable(deliveriesGroupedByProductTypePaId.entrySet())
                .map(mapEntry -> {
                    List<PaperDelivery> filteredList;
                    Integer limit = senderLimitMap.get(mapEntry.getKey());
                    if (Objects.nonNull(limit)) {
                        filteredList = mapEntry.getValue().stream().limit(senderLimitMap.get(mapEntry.getKey())).toList();
                    } else {
                        filteredList = mapEntry.getValue();
                    }
                    if (!CollectionUtils.isEmpty(filteredList)) {
                        senderLimitJobPaperDeliveries.getSendToNextStep().addAll(mapItemForEvaluateDriverCapacityStep(filteredList, deliveryWeek));
                    }
                    if (mapEntry.getValue().size() > filteredList.size()) {
                        senderLimitJobPaperDeliveries.getSendToResidualStep().addAll(mapEntry.getValue().subList(filteredList.size(), mapEntry.getValue().size()));
                    }
                    return filteredList.size();
                })
                .then().thenReturn(senderLimitJobPaperDeliveries);
    }

    private List<PaperDelivery> mapItemForEvaluateDriverCapacityStep(List<PaperDelivery> filteredList, LocalDate deliveryWeek) {
        return new ArrayList<>();
    }

    public Mono<Map<String, Integer>> calculateSenderLimit(LocalDate deliveryDate, Integer declaredCapacity, String province, Set<String> paIdProductTypeTuples, Map<String, Integer> residualSenderLimitMap, Map<String, Integer> senderLimitMap) {
        List<String> senderLimitPkList = paIdProductTypeTuples.stream()
                .map(tuple -> tuple + "~" + province)
                .toList();

        paIdProductTypeTuples.removeIf(residualSenderLimitMap::containsKey);

        return Flux.fromIterable(senderLimitPkList).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, deliveryDate)
                        .doOnNext(paperDeliverySenderLimit -> {
                            Integer limit = (declaredCapacity * paperDeliverySenderLimit.getPercentageLimit()) / 100;
                            residualSenderLimitMap.put(paperDeliverySenderLimit.getPk(), limit);
                            senderLimitMap.put(paperDeliverySenderLimit.getPk(), limit);
                        }))
                .then(Mono.just(senderLimitMap));
    }

    public Mono<List<PaperDelivery>> retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity(List<PaperDelivery> paperDelivery, String tenderId, List<String> drivers, Map<Integer, List<String>> priorityMap) {
        List<PaperDelivery> toSenderLimitEvaluation = new ArrayList<>();
        if (drivers.size() == 1) {
            String unifiedDeliveryDriver = drivers.getFirst();
            return Mono.just(paperDeliveryUtils.buildtoDriverCapacityEvaluation(paperDelivery, unifiedDeliveryDriver, tenderId, priorityMap));
        } else {
            Map<String, List<PaperDelivery>> groupedByCapProductType = paperDelivery.stream().collect(Collectors.groupingBy(o -> o.getCap() + "~" + o.getProductType()));
            List<DeliveryDriverRequest> deliveryDriverRequest = new ArrayList<>();
            return Flux.fromIterable(groupedByCapProductType.entrySet())
                    .doOnNext(capProductTypeEntry ->
                        cacheService.getFromCache(capProductTypeEntry.getKey())
                                .ifPresentOrElse(unifiedDeliveryDriver -> toSenderLimitEvaluation.addAll(paperDeliveryUtils.buildtoDriverCapacityEvaluation(capProductTypeEntry.getValue(), unifiedDeliveryDriver, tenderId, priorityMap)),
                                        () -> deliveryDriverRequest.add(new DeliveryDriverRequest(capProductTypeEntry.getKey().split("~")[0], capProductTypeEntry.getKey().split("~")[1])))      )
                    .then(Mono.just(deliveryDriverRequest))
                    .map(paperChannelRequests -> retrieveUnifiedDeliveryDriversFromPaperChannel(paperChannelRequests, tenderId))
                    .doOnNext(this::insertInCache)
                    .map(paperChannelDeliveryDriverResponses -> paperDeliveryUtils.assignUnifiedDeliveryDriverAndBuildNewStepEntities(paperChannelDeliveryDriverResponses, groupedByCapProductType, tenderId, priorityMap))
                    .doOnNext(toSenderLimitEvaluation::addAll)
                    .thenReturn(toSenderLimitEvaluation);
        }
    }

    private void insertInCache(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses) {
        Map<String, String> map = paperDeliveryUtils.groupByGeoKeyAndProduct(paperChannelDeliveryDriverResponses);
        map.forEach(cacheService::addToCache);
    }

    private List<PaperChannelDeliveryDriverResponse> retrieveUnifiedDeliveryDriversFromPaperChannel(List<DeliveryDriverRequest> deliveryDriverRequests, String tenderId) {
        try {
            SdkBytes sdkBytesResponse = lambdaClient.invoke(InvokeRequest.builder()
                            .functionName(pnDelayerConfigs.getPaperChannelTenderApiLambdaName())
                            .invocationType(InvocationType.REQUEST_RESPONSE)
                            .payload(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(paperDeliveryUtils.constructPaperChannelTenderApiPayload(deliveryDriverRequests, tenderId))))
                            .build())
                    .payload();
            return objectMapper.readValue(sdkBytesResponse.asByteArray(), new com.fasterxml.jackson.core.type.TypeReference<>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Mono<Map<String, Integer>> calculateSenderLimit(LocalDate deliveryDate, Map<String, Integer> capacityMap, String province, List<String> paIdProductTypeTuples) {
        Map<String, Integer> senderLimitMap = new HashMap<>();
        List<String> senderLimitPkList = paIdProductTypeTuples.stream()
                .map(tuple -> tuple + "~" + province)
                .toList();
        return Flux.fromIterable(senderLimitPkList).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, deliveryDate)
                        .doOnNext(paperDeliverySenderLimit -> {
                            Integer limit = (capacityMap.get(paperDeliverySenderLimit.getProductType()) * paperDeliverySenderLimit.getPercentageLimit()) / 100;
                            senderLimitMap.put(paperDeliverySenderLimit.getPk(), limit);
                        }))
                .then(Mono.just(senderLimitMap));
    }
}

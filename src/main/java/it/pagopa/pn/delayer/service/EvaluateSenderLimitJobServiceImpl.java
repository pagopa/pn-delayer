package it.pagopa.pn.delayer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
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
@RequiredArgsConstructor
public class EvaluateSenderLimitJobServiceImpl implements EvaluateSenderLimitJobService {

    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final CapProductTypeDriverCacheService cacheService;
    private final PaperDeliveryUtils paperDeliveryUtils;
    private final LambdaClient lambdaClient;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> startSenderLimitJob(String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecution) {
        return Mono.empty();
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
}

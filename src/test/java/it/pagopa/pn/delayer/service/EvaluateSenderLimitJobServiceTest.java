package it.pagopa.pn.delayer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EvaluateSenderLimitJobServiceTest {

    @Mock
    private PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @Mock
    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private CapProductTypeDriverCacheService cacheService;

    @Mock
    private LambdaClient lambdaClient;

    private EvaluateSenderLimitJobServiceImpl evaluateSenderLimitJobService;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        evaluateSenderLimitJobService = new EvaluateSenderLimitJobServiceImpl(
                paperDeliverySenderLimitDAO,
                pnDelayerConfigs,
                cacheService,
                paperDeliveryUtils,
                lambdaClient,
                objectMapper
        );
    }

    @Test
    void calculateSenderLimit_returnsCorrectSenderLimitMap() {
        LocalDate deliveryDate = LocalDate.now();
        Map<String, Integer> capacityMap = Map.of("ProductA", 100, "ProductB", 200);
        String province = "RM";
        List<String> paIdProductTypeTuples = List.of("PA1~ProductA", "PA2~ProductB", "PA2~ProductA");

        PaperDeliverySenderLimit limit1 = new PaperDeliverySenderLimit();
        limit1.setPk("PA1~ProductA~RM");
        limit1.setProductType("ProductA");
        limit1.setPercentageLimit(20);
        limit1.setDeliveryDate("2025-02-03");
        limit1.setProvince("RM");
        limit1.setPaId("PA1");

        PaperDeliverySenderLimit limit2 = new PaperDeliverySenderLimit();
        limit2.setPk("PA2~ProductA~RM");
        limit2.setProductType("ProductA");
        limit2.setPercentageLimit(50);
        limit2.setDeliveryDate("2025-02-03");
        limit2.setProvince("RM");
        limit2.setPaId("PA2");

        PaperDeliverySenderLimit limit3 = new PaperDeliverySenderLimit();
        limit3.setPk("PA2~ProductB~RM");
        limit3.setProductType("ProductB");
        limit3.setPercentageLimit(25);
        limit3.setDeliveryDate("2025-02-03");
        limit3.setProvince("RM");
        limit3.setPaId("PA2");

        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), eq(deliveryDate)))
                .thenReturn(Flux.just(limit1, limit2, limit3));

        StepVerifier.create(evaluateSenderLimitJobService.calculateSenderLimit(deliveryDate, capacityMap, province, paIdProductTypeTuples))
                .expectNextMatches(senderLimitMap ->
                        senderLimitMap.get("PA1~ProductA~RM") == 20 &&
                                senderLimitMap.get("PA2~ProductB~RM") == 50 &&
                        senderLimitMap.get("PA2~ProductA~RM") == 50)
                .verifyComplete();
    }

    @Test
    void retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity_handlesSingleDriver() {
        List<PaperDelivery> paperDeliveries = List.of(new PaperDelivery());
        String tenderId = "tender1";
        List<String> drivers = List.of("driver1");
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("DEFAULT"));

        when(paperDeliveryUtils.buildtoDriverCapacityEvaluation(paperDeliveries, "driver1", tenderId, priorityMap))
                .thenReturn(List.of(new PaperDelivery()));

        StepVerifier.create(evaluateSenderLimitJobService.retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity(paperDeliveries, tenderId, drivers, priorityMap))
                .expectNextMatches(result -> result.size() == 1)
                .verifyComplete();
    }

    @Test
    void retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity_handlesMultipleDriversWithCacheHit() {
        List<PaperDelivery> paperDeliveries = List.of(new PaperDelivery());
        String tenderId = "tender1";
        List<String> drivers = List.of("driver1", "driver2");
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("DEFAULT"));

        when(cacheService.getFromCache(anyString())).thenReturn(Optional.of("driver1"));
        when(paperDeliveryUtils.buildtoDriverCapacityEvaluation(anyList(), eq("driver1"), eq(tenderId), eq(priorityMap)))
                .thenReturn(List.of(new PaperDelivery()));

        String responseString = "[{\"unifiedDeliveryDriver\":\"driver1\",\"geoKey\":\"geoKey1\",\"product\":\"AR\"}]";
        when(lambdaClient.invoke((InvokeRequest) any())).thenReturn(InvokeResponse.builder()
                .payload(SdkBytes.fromByteArray(responseString.getBytes(StandardCharsets.UTF_8)))
                .build());

        StepVerifier.create(evaluateSenderLimitJobService.retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity(paperDeliveries, tenderId, drivers, priorityMap))
                .expectNextMatches(result -> result.size() == 1)
                .verifyComplete();
    }

    @Test
    void retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity_handlesMultipleDriversWithCacheMiss() {
        List<PaperDelivery> paperDeliveries = List.of(new PaperDelivery());
        String tenderId = "tender1";
        List<String> drivers = List.of("driver1", "driver2");
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("DEFAULT"));

        when(cacheService.getFromCache(anyString())).thenReturn(Optional.empty());
        String responseString = "[{\"unifiedDeliveryDriver\":\"driver1\",\"geoKey\":\"geoKey1\",\"product\":\"AR\"}]";
        when(lambdaClient.invoke((InvokeRequest) any())).thenReturn(InvokeResponse.builder()
                .payload(SdkBytes.fromByteArray(responseString.getBytes(StandardCharsets.UTF_8)))
                .build());        when(paperDeliveryUtils.assignUnifiedDeliveryDriverAndBuildNewStepEntities(anyList(), anyMap(), eq(tenderId), eq(priorityMap)))
                .thenReturn(List.of(new PaperDelivery()));

        StepVerifier.create(evaluateSenderLimitJobService.retrieveUnifiedDeliveryDriverAndConstructHighPriorityEntity(paperDeliveries, tenderId, drivers, priorityMap))
                .expectNextMatches(result -> result.size() == 1)
                .verifyComplete();
    }
}

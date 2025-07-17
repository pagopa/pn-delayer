package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.utils.DeliveryDriverUtils;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import it.pagopa.pn.delayer.utils.PnDelayerUtils;
import it.pagopa.pn.delayer.utils.SenderLimitUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EvaluateSenderLimitJobServiceTest {

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @Mock
    private PnDelayerUtils pnDelayerUtils;

    @Mock
    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private DeliveryDriverUtils deliveryDriverUtils;

    @Mock
    private SsmParameterConsumerActivation ssmParameterConsumerActivation;

    @Mock
    private SenderLimitUtils senderLimitUtils;

    private EvaluateSenderLimitJobServiceImpl evaluateSenderLimitJobService;

    @BeforeEach
    void setUp() {
        evaluateSenderLimitJobService = new EvaluateSenderLimitJobServiceImpl(
                pnDelayerUtils,
                pnDelayerConfigs,
                paperDeliveryUtils,
                deliveryDriverUtils,
                ssmParameterConsumerActivation,
                senderLimitUtils
        );
    }

    @Test
    void startSenderLimitJob_provinceWithOneDeliveryDriver() {
        String province = "RM";
        String tenderId = "tender1";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        Instant startExecutionBatch = Instant.now();
        LocalDate deliveryWeek = LocalDate.now();
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("DEFAULT"));

        when(pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch)).thenReturn(deliveryWeek);
        when(ssmParameterConsumerActivation.getParameterValue(anyString(), eq(Map.class))).thenReturn(Optional.of(priorityMap));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .thenReturn(Mono.just(new DriversTotalCapacity(1, List.of("driver1"))));

        StepVerifier.create(evaluateSenderLimitJobService.startSenderLimitJob(province, tenderId, lastEvaluatedKey, startExecutionBatch))
                .verifyComplete();
    }

    @Test
    void startSenderLimitJob_provinceWithTwoDeliveryDriver() {
        String province = "RM";
        String tenderId = "tender1";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        Instant startExecutionBatch = Instant.now();
        LocalDate deliveryWeek = LocalDate.now();
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("DEFAULT"));

        when(pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch)).thenReturn(deliveryWeek);
        when(ssmParameterConsumerActivation.getParameterValue(anyString(), eq(Map.class))).thenReturn(Optional.of(priorityMap));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .thenReturn(Mono.just(new DriversTotalCapacity(1, List.of("driver1"))));

        StepVerifier.create(evaluateSenderLimitJobService.startSenderLimitJob(province, tenderId, lastEvaluatedKey, startExecutionBatch))
                .verifyComplete();
    }
}

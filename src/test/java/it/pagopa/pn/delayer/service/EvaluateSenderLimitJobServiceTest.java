/*
package it.pagopa.pn.delayer.service;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
import it.pagopa.pn.delayer.utils.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY;
import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_SENDER_LIMIT;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EvaluateSenderLimitJobServiceTest {

    @InjectMocks
    private EvaluateSenderLimitJobServiceImpl service;

    @Mock
    private PnDelayerUtils pnDelayerUtils;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @Mock
    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private DeliveryDriverUtils deliveryDriverUtils;

    @Mock
    private SsmParameterConsumerActivation ssmParameterConsumerActivation;

    @Mock
    private SenderLimitUtils senderLimitUtils;

    private final String province = "RM";
    private final String tenderId = "TENDER1";
    private final Instant now = Instant.now();
    private final LocalDate deliveryWeek = LocalDate.now();
    private final Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();

    @BeforeEach
    void setupMocks() {
        when(pnDelayerUtils.calculateDeliveryWeek(now)).thenReturn(deliveryWeek);
        when(pnDelayerConfigs.getPaperDeliveryPriorityParameterName()).thenReturn("priority-param");

        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("AR"), 2, List.of("890"));
        when(ssmParameterConsumerActivation.getParameterValue(eq("priority-param"), eq(Map.class)))
                .thenReturn(Optional.of(priorityMap));
    }

    @Test
    void startSenderLimitJob_singleDriver_success() {
        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("Driver1"), Map.of());
        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        List<PaperDelivery> deliveries2 = getPaperDeliveries(true);
        List<PaperDelivery> deliveries3 = getPaperDeliveries(false);
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_RESIDUAL_CAPACITY).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("driver1~RM~2025-01-01T00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(deliveries2);
        when(page2.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        Page<PaperDelivery> page3 = mock(Page.class);
        when(page3.items()).thenReturn(deliveries3);
        when(page3.lastEvaluatedKey()).thenReturn(new HashMap<>());

        when(paperDeliveryUtils.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", province, "2025-07-07")), any(), eq(3)))
                .thenReturn(Mono.just(page));

        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .thenReturn(Mono.just(capacity));

        when(pnDelayerUtils.excludeRsAndSecondAttempt(any(), any())).thenReturn(mockItems);
        when(pnDelayerUtils.groupByPaIdProductTypeProvince(any())).thenReturn(Map.of());

        when(paperDeliveryUtils.insertPaperDeliveries(any(), any()))
                .thenReturn(Mono.just(mockItems));
        when(senderLimitUtils.updateUsedSenderLimit(any(), any(), any()))
                .thenReturn(Mono.empty());

        when(senderLimitUtils.retrieveAndEvaluateSenderLimit(any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());

        when(pnDelayerConfigs.getDao()).thenReturn(mock(PnDelayerConfigs.DaoConfig.class));
        when(pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()).thenReturn(50);

        when(pnDelayerUtils.enrichWithPriorityAndUnifiedDeliveryDriver(any(), any(), any(), any()))
                .thenReturn(mockItems);

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, lastEvaluatedKey, now))
                .verifyComplete();
    }

    @Test
    void startSenderLimitJob_multipleDrivers_withCacheAndFallback() {
        List<String> drivers = List.of("driver1", "driver2");
        DriversTotalCapacity capacity = new DriversTotalCapacity(drivers, Map.of());
        List<PaperDelivery> paperDeliveries = List.of(mock(PaperDelivery.class));
        PaperDeliveryPage page = new PaperDeliveryPage(paperDeliveries, Map.of());

        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .thenReturn(Mono.just(capacity));
        when(paperDeliveryUtils.retrievePaperDeliveries(any(), any(), any(), any(), anyInt()))
                .thenReturn(Mono.just(page));

        Map<String, List<PaperDelivery>> grouped = Map.of("00100~AR", paperDeliveries);
        when(pnDelayerUtils.groupByCapAndProductType(any())).thenReturn(grouped);

        when(deliveryDriverUtils.retrieveFromCache(any())).thenReturn(Optional.empty());
        when(deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(any()))
                .thenReturn(Map.of("00100~AR", "driver2"));
        doNothing().when(deliveryDriverUtils).insertInCache(any());

        when(pnDelayerUtils.assignUnifiedDeliveryDriverAndBuildNewStepEntities(any(), any(), any(), any()))
                .thenReturn(paperDeliveries);

        when(pnDelayerUtils.excludeRsAndSecondAttempt(any(), any())).thenReturn(paperDeliveries);
        when(pnDelayerUtils.groupByPaIdProductTypeProvince(any())).thenReturn(Map.of());
        when(senderLimitUtils.retrieveAndEvaluateSenderLimit(any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());

        when(paperDeliveryUtils.insertPaperDeliveries(any(), any())).thenReturn(Mono.just(paperDeliveries));
        when(senderLimitUtils.updateUsedSenderLimit(any(), any(), any())).thenReturn(Mono.empty());

        when(pnDelayerConfigs.getDao()).thenReturn(mock(PnDelayerConfigs.DaoConfig.class));
        when(pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()).thenReturn(50);

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, lastEvaluatedKey, now))
                .verifyComplete();
    }


    @Test
    void startSenderLimitJob_withPagination_callsRecursive() {
        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("Driver1"), Map.of());
        List<PaperDelivery> mockItems = List.of(new PaperDelivery());
        Map<String, AttributeValue> nextPageKey = Map.of("key", AttributeValue.builder().s("val").build());

        PaperDeliveryPage page1 = new PaperDeliveryPage(mockItems, nextPageKey);
        PaperDeliveryPage page2 = new PaperDeliveryPage(mockItems, Map.of());

        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryWeek, tenderId, province))
                .thenReturn(Mono.just(capacity));

        when(paperDeliveryUtils.retrievePaperDeliveries(any(), any(), any(), eq(lastEvaluatedKey), anyInt()))
                .thenReturn(Mono.just(page1));
        when(paperDeliveryUtils.retrievePaperDeliveries(any(), any(), any(), eq(nextPageKey), anyInt()))
                .thenReturn(Mono.just(page2));

        when(pnDelayerUtils.excludeRsAndSecondAttempt(any(), any())).thenReturn(mockItems);
        when(pnDelayerUtils.groupByPaIdProductTypeProvince(any())).thenReturn(Map.of());

        when(paperDeliveryUtils.insertPaperDeliveries(any(), any()))
                .thenReturn(Mono.just(mockItems));
        when(senderLimitUtils.updateUsedSenderLimit(any(), any(), any()))
                .thenReturn(Mono.empty());

        when(senderLimitUtils.retrieveAndEvaluateSenderLimit(any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());

        when(pnDelayerConfigs.getDao()).thenReturn(mock(PnDelayerConfigs.DaoConfig.class));
        when(pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit()).thenReturn(50);
        when(pnDelayerUtils.enrichWithPriorityAndUnifiedDeliveryDriver(any(), any(), any(), any()))
                .thenReturn(mockItems);

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, lastEvaluatedKey, now))
                .verifyComplete();

        verify(paperDeliveryUtils, times(2)).retrievePaperDeliveries(any(), any(), any(), any(), anyInt());
    }

    private static @NotNull List<PaperDelivery> getPaperDeliveries(boolean sameCap) {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("driver1~RM~2025-01-01T00:00:00Z~requestId1");
        paperDelivery1.setPriority(1);
        paperDelivery1.setRequestId(UUID.randomUUID().toString());
        paperDelivery1.setCreatedAt(Instant.now().toString());
        paperDelivery1.setNotificationSentAt("2025-01-01T00:00:00Z");
        paperDelivery1.setPrepareRequestDate("2025-01-02T00:00:00Z");
        paperDelivery1.setProductType("RS");
        paperDelivery1.setAttempt(0);
        paperDelivery1.setProvince("RM");
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("driver1~RM~2025-01-01T00:00:00Z~requestId2");
        paperDelivery2.setPriority(3);
        paperDelivery2.setRequestId(UUID.randomUUID().toString());
        paperDelivery2.setNotificationSentAt("2025-01-01T00:00:00Z");
        paperDelivery2.setPrepareRequestDate("2025-01-02T00:00:00Z");
        paperDelivery2.setProductType("AR");
        paperDelivery2.setAttempt(0);
        paperDelivery2.setProvince("RM");
        paperDelivery2.setCreatedAt(Instant.now().toString());
        if (sameCap) {
            paperDelivery1.setCap("00184");
            paperDelivery2.setCap("00184");
        } else {
            paperDelivery1.setCap("00185");
            paperDelivery2.setCap("00184");
        }
        return List.of(paperDelivery1, paperDelivery2);
    }
}
*/

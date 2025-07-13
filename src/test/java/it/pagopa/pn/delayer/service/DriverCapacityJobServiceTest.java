package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_SENDER_LIMIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DriverCapacityJobServiceTest {

    @Mock
    private PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;

    @Mock
    private PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;

    @Mock
    private PaperDeliveryDAO paperDeliveryDAO;

    private DriverCapacityJobServiceImpl driverCapacityJobService;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs.Dao dao = new PnDelayerConfigs.Dao();
        dao.setPaperDeliveryQueryLimit(5);
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        pnDelayerConfigs.setDeliveryDateInterval(Duration.ofDays(7));
        pnDelayerConfigs.setDeliveryDateDayOfWeek(1);
        pnDelayerConfigs.setDao(dao);

        driverCapacityJobService = new DriverCapacityJobServiceImpl(
                paperDeliveryUsedCapacityDAO,
                paperDeliveryCapacityDAO,
                paperDeliveryDAO,
                new PaperDeliveryUtils(pnDelayerConfigs),
                pnDelayerConfigs
        );
    }

    @Test
    void startEvaluateDriverCapacityJob_NoProvinceCapacity() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");
        String tenderId = "tender123";
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(10, 10);
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(capacityTuple));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        verifyNoInteractions(paperDeliveryDAO);
        verifyNoInteractions(paperDeliveryCapacityDAO);
    }

    @Test
    void startEvaluateDriverCapacityJob_NoItemsToProcess() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");
        String tenderId = "tender123";
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(10, 0);
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(capacityTuple));
        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(),
                eq(5)))
                .thenReturn(Mono.just(Page.create(Collections.emptyList())));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(),
                eq(5));
    }

    @Test
    void startEvaluateDriverCapacityJob_NoCapCapacityForOneCap_WithoutLastEvaluatedKey() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");
        String tenderId = "tender123";
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(10, 0);
        Tuple2<Integer, Integer> capTuple = Tuples.of(5, 5);
        Tuple2<Integer, Integer> capTuple2 = Tuples.of(5, 0);
        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(capacityTuple));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00185"), any())).thenReturn(Mono.just(capTuple));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00184"), any())).thenReturn(Mono.just(capTuple2));
        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5)))
                .thenReturn(Mono.just(Page.create(deliveries)));
        when(paperDeliveryDAO.insertPaperDeliveries(anyList())).thenReturn(Mono.empty());
        when(paperDeliveryUsedCapacityDAO.updateCounter(any(), any(), anyInt(), any(), anyInt())).thenReturn(Mono.just(1));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(),
                eq(5));
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq(province), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq("00185"), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq("00184"), any());
        verify(paperDeliveryUsedCapacityDAO).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(1), any(), eq(10));
        verify(paperDeliveryUsedCapacityDAO).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(1), any(), eq(5));
    }

    @Test
    void startEvaluateDriverCapacityJob_NoCapCapacityForOneCap_WithLastEvaluatedKey() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");
        String tenderId = "tender123";
        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        List<PaperDelivery> deliveries2 = getPaperDeliveries(true);


        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(Tuples.of(5, 0)));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00185"), any()))
                .thenReturn(Mono.just(Tuples.of(1, 1)));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00184"), any()))
                .thenReturn(Mono.empty())
                .thenReturn(Mono.just(Tuples.of(3, 1)));
        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(eq(tenderId), eq(unifiedDeliveryDriver), eq("00184"), any()))
                .thenReturn(Mono.just(3));

        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_SENDER_LIMIT).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("RM~2025-01-01t00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5)))
                .thenReturn(Mono.just(page));

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(4)))
                .thenReturn(Mono.just(Page.create(deliveries2)));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList())).thenReturn(Mono.empty());
        when(paperDeliveryUsedCapacityDAO.updateCounter(any(), any(), anyInt(), any(), anyInt())).thenReturn(Mono.just(1));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5));
        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(4));
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq(province), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq("00185"), any());
        verify(paperDeliveryUsedCapacityDAO, times(2)).get(eq(unifiedDeliveryDriver), eq("00184"), any());
        verify(paperDeliveryUsedCapacityDAO).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(1), any(), eq(5));
        verify(paperDeliveryUsedCapacityDAO).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(2), any(), eq(5));
        verify(paperDeliveryUsedCapacityDAO).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(1), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(2), any(), eq(3));
    }

    @Test
    void startEvaluateDriverCapacityJob_NoCapCapacityForOneCap_WithLastEvaluatedKey_WithoutProvinceCapacity() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");
        String tenderId = "tender123";
        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        List<PaperDelivery> deliveries2 = getPaperDeliveries(true);

        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(Tuples.of(3, 0)));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00185"), any()))
                .thenReturn(Mono.just(Tuples.of(1, 1)));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00184"), any()))
                .thenReturn(Mono.empty())
                .thenReturn(Mono.just(Tuples.of(3, 1)));
        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(eq(tenderId), eq(unifiedDeliveryDriver), eq("00184"), any()))
                .thenReturn(Mono.just(3));

        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_SENDER_LIMIT).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("RM~2025-01-01t00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(deliveries2);
        when(page2.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(3)))
                .thenReturn(Mono.just(page));

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(2)))
                .thenReturn(Mono.just(page2));

        ArgumentCaptor<List<PaperDelivery>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDAO.insertPaperDeliveries(argumentCaptor.capture())).thenReturn(Mono.empty());
        when(paperDeliveryUsedCapacityDAO.updateCounter(any(), any(), anyInt(), any(), anyInt())).thenReturn(Mono.just(1));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(3));
        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(2));
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq(province), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq("00185"), any());
        verify(paperDeliveryUsedCapacityDAO, times(2)).get(eq(unifiedDeliveryDriver), eq("00184"), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(1), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(2), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(1), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(2), any(), eq(3));
        List<List<PaperDelivery>> capturedDeliveries = argumentCaptor.getAllValues();
        Assertions.assertEquals(1, capturedDeliveries.getFirst().size());
        Assertions.assertEquals(2, capturedDeliveries.getLast().size());
        verify(paperDeliveryDAO, times(2)).insertPaperDeliveries(anyList());
    }

    private static @NotNull List<PaperDelivery> getPaperDeliveries(boolean sameCap) {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        paperDelivery1.setPriority(1);
        paperDelivery1.setRequestId("requestId1");
        paperDelivery1.setCreatedAt(Instant.now().toString());
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery2.setPriority(1);
        paperDelivery2.setRequestId("requestId2");
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

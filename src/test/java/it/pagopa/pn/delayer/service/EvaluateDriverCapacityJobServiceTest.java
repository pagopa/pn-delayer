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
import java.util.*;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EvaluateDriverCapacityJobServiceTest {

    @Mock
    private PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;

    @Mock
    private PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;

    @Mock
    private PaperDeliveryDAO paperDeliveryDAO;

    private EvaluateDriverCapacityJobServiceImpl driverCapacityJobService;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs.Dao dao = new PnDelayerConfigs.Dao();
        dao.setPaperDeliveryQueryLimit(5);
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        pnDelayerConfigs.setDeliveryDateInterval(Duration.ofDays(7));
        pnDelayerConfigs.setDeliveryDateDayOfWeek(1);
        pnDelayerConfigs.setDao(dao);

        PaperDeliveryUtils paperDeliveryUtils = new PaperDeliveryUtils(pnDelayerConfigs);
        DeliveryDriverCapacityService deliveryDriverCapacityService = new DeliveryDriverCapacityService(paperDeliveryUsedCapacityDAO, paperDeliveryCapacityDAO);
        driverCapacityJobService = new EvaluateDriverCapacityJobServiceImpl(new PaperDeliveryService(paperDeliveryDAO, pnDelayerConfigs, paperDeliveryUtils,deliveryDriverCapacityService), new PaperDeliveryUtils(pnDelayerConfigs));
    }

    @Test
    void startEvaluateDriverCapacityJob_NoProvinceCapacity() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2025-01-06T00:00:00Z");
        String tenderId = "tender123";
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(10, 10);
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(capacityTuple));

        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5)))
                .thenReturn(Mono.just(Page.create(deliveries)));
        ArgumentCaptor<List<PaperDelivery>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDAO.insertPaperDeliveries(argumentCaptor.capture())).thenReturn(Mono.empty());
        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        List<PaperDelivery> capturedDeliveries = argumentCaptor.getValue();
        Assertions.assertEquals(2, capturedDeliveries.size());
        Assertions.assertTrue(capturedDeliveries.getFirst().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getLast().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getFirst().getSk().startsWith(String.join("~", province, "2025-01-02T00:00:00Z")));
        Assertions.assertTrue(capturedDeliveries.getLast().getSk().startsWith(String.join("~", province, "2025-01-01T00:00:00Z")));
        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(
                eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY),
                any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(),
                eq(5));
        verify(paperDeliveryDAO, times(1)).insertPaperDeliveries(anyList());
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
                eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY),
                any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(),
                eq(5)))
                .thenReturn(Mono.just(Page.create(Collections.emptyList())));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(
                eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY),
                any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(),
                eq(5));
        verify(paperDeliveryDAO, times(0)).insertPaperDeliveries(anyList());
    }

    @Test
    void startEvaluateDriverCapacityJob_NoCapCapacityForOneCap_WithoutLastEvaluatedKey() {
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Instant startExecutionBatch = Instant.parse("2025-01-07T00:00:00Z");
        String tenderId = "tender123";
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(10, 0);
        Tuple2<Integer, Integer> capTuple = Tuples.of(5, 5);
        Tuple2<Integer, Integer> capTuple2 = Tuples.of(5, 0);
        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq(province), any())).thenReturn(Mono.just(capacityTuple));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00185"), any())).thenReturn(Mono.just(capTuple));
        when(paperDeliveryUsedCapacityDAO.get(eq(unifiedDeliveryDriver), eq("00184"), any())).thenReturn(Mono.just(capTuple2));
        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5)))
                .thenReturn(Mono.just(Page.create(deliveries)));

        ArgumentCaptor<List<PaperDelivery>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDAO.insertPaperDeliveries(argumentCaptor.capture())).thenReturn(Mono.empty());
        when(paperDeliveryUsedCapacityDAO.updateCounter(any(), any(), anyInt(), any(), anyInt())).thenReturn(Mono.just(1));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = argumentCaptor.getAllValues();
        Assertions.assertEquals(1, capturedDeliveries.getFirst().size());
        Assertions.assertTrue(capturedDeliveries.getFirst().getFirst().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getFirst().getFirst().getSk().startsWith(String.join("~", province, "2025-01-02T00:00:00Z")));

        Assertions.assertEquals(1, capturedDeliveries.getLast().size());
        Assertions.assertTrue(capturedDeliveries.getLast().getLast().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.getLast().getLast().getSk().startsWith("3~"));

        verify(paperDeliveryDAO, times(2)).insertPaperDeliveries(anyList());
        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(
                eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY),
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
        Instant startExecutionBatch = Instant.parse("2025-01-07T10:00:00Z");
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
        ArgumentCaptor<List<PaperDelivery>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDAO.insertPaperDeliveries(argumentCaptor.capture())).thenReturn(Mono.empty());

        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-06~" + EVALUATE_DRIVER_CAPACITY).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("RM~2025-01-01t00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5)))
                .thenReturn(Mono.just(page));

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(4)))
                .thenReturn(Mono.just(Page.create(deliveries2)));

        when(paperDeliveryUsedCapacityDAO.updateCounter(any(), any(), anyInt(), any(), anyInt())).thenReturn(Mono.just(1));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = argumentCaptor.getAllValues();
        Assertions.assertEquals(1, capturedDeliveries.getFirst().size());
        Assertions.assertTrue(capturedDeliveries.getFirst().getFirst().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getFirst().getFirst().getSk().startsWith(String.join("~", province, "2025-01-02T00:00:00Z")));

        Assertions.assertEquals(1, capturedDeliveries.get(1).size());
        Assertions.assertTrue(capturedDeliveries.get(1).getFirst().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.get(1).getFirst().getSk().startsWith(String.join("~", "3")));

        Assertions.assertEquals(2, capturedDeliveries.getLast().size());
        Assertions.assertTrue(capturedDeliveries.getLast().getFirst().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.getLast().getFirst().getSk().startsWith(String.join("3~")));
        Assertions.assertTrue(capturedDeliveries.getLast().getLast().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.getLast().getLast().getSk().startsWith(String.join("3~")));

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5));
        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(4));
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
        Instant startExecutionBatch = Instant.parse("2025-01-06T00:00:00Z");
        String tenderId = "tender123";
        List<PaperDelivery> deliveries = getPaperDeliveries(false);
        List<PaperDelivery> deliveries2 = getPaperDeliveries(true);
        List<PaperDelivery> deliveries3 = getPaperDeliveries(false);

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
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_DRIVER_CAPACITY).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("driver1~RM~2025-01-01T00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(deliveries2);
        when(page2.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);

        Page<PaperDelivery> page3 = mock(Page.class);
        when(page3.items()).thenReturn(deliveries3);
        when(page3.lastEvaluatedKey()).thenReturn(new HashMap<>());

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(3)))
                .thenReturn(Mono.just(page));

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(2)))
                .thenReturn(Mono.just(page2));

        when(paperDeliveryDAO.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(5)))
                .thenReturn(Mono.just(page3));

        ArgumentCaptor<List<PaperDelivery>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDAO.insertPaperDeliveries(argumentCaptor.capture())).thenReturn(Mono.empty());
        when(paperDeliveryUsedCapacityDAO.updateCounter(any(), any(), anyInt(), any(), anyInt())).thenReturn(Mono.just(1));

        StepVerifier.create(driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, null, startExecutionBatch, tenderId))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = argumentCaptor.getAllValues();
        Assertions.assertEquals(1, capturedDeliveries.getFirst().size());
        Assertions.assertTrue(capturedDeliveries.getFirst().getFirst().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getFirst().getFirst().getSk().startsWith(String.join("~", province, "2025-01-02T00:00:00Z")));

        Assertions.assertEquals(1, capturedDeliveries.get(1).size());
        Assertions.assertTrue(capturedDeliveries.get(1).getFirst().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.get(1).getFirst().getSk().startsWith(String.join("~", "3")));

        Assertions.assertEquals(2, capturedDeliveries.get(2).size());
        Assertions.assertTrue(capturedDeliveries.get(2).getFirst().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.get(2).getFirst().getSk().startsWith(String.join("~", "1")));
        Assertions.assertTrue(capturedDeliveries.get(2).getLast().getPk().equalsIgnoreCase("2025-01-06~" + EVALUATE_PRINT_CAPACITY));
        Assertions.assertTrue(capturedDeliveries.get(2).getLast().getSk().startsWith(String.join("~", "3")));

        Assertions.assertEquals(2, capturedDeliveries.getLast().size());
        Assertions.assertTrue(capturedDeliveries.getLast().getFirst().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getLast().getFirst().getSk().startsWith(String.join("~", province, "2025-01-02T00:00:00Z")));
        Assertions.assertTrue(capturedDeliveries.getLast().getLast().getPk().endsWith("2025-01-13~" + EVALUATE_SENDER_LIMIT));
        Assertions.assertTrue(capturedDeliveries.getLast().getLast().getSk().startsWith(String.join("~", province, "2025-01-01T00:00:00Z")));

        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(3));
        verify(paperDeliveryDAO, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY), any(), eq(String.join("~", unifiedDeliveryDriver, province)), any(), eq(2));
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq(province), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).get(eq(unifiedDeliveryDriver), eq("00185"), any());
        verify(paperDeliveryUsedCapacityDAO, times(2)).get(eq(unifiedDeliveryDriver), eq("00184"), any());
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(1), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq(province), eq(2), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(1), any(), eq(3));
        verify(paperDeliveryUsedCapacityDAO, times(1)).updateCounter(eq(unifiedDeliveryDriver), eq("00184"), eq(2), any(), eq(3));
        verify(paperDeliveryDAO, times(4)).insertPaperDeliveries(anyList());
    }

    private static @NotNull List<PaperDelivery> getPaperDeliveries(boolean sameCap) {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_DRIVER_CAPACITY);
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
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_DRIVER_CAPACITY);
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

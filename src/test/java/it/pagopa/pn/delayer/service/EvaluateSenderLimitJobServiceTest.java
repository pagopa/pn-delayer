package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.config.SsmParameterConsumerActivation;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriver;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.*;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY;
import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_SENDER_LIMIT;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EvaluateSenderLimitJobServiceTest {

    private EvaluateSenderLimitJobServiceImpl service;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @Mock
    private DeliveryDriverUtils deliveryDriverUtils;

    @Mock
    private PaperDeliveryDAO paperDeliveryDao;

    @Mock
    private SsmParameterConsumerActivation ssmParameterConsumerActivation;

    @Mock
    private PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @Mock
    private PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    private final String province = "RM";
    private final String tenderId = "TENDERID";

    @BeforeEach
    void setupMocks() {
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        pnDelayerConfigs.setDeliveryDateDayOfWeek(1);
        pnDelayerConfigs.setPaperDeliveryPriorityParameterName("priority-param");
        PnDelayerUtils pnDelayerUtils = new PnDelayerUtils(pnDelayerConfigs, new PrintCapacityUtils(pnDelayerConfigs));
        PnDelayerConfigs.Dao daoConfig = new PnDelayerConfigs.Dao();
        daoConfig.setPaperDeliveryQueryLimit(50);
        pnDelayerConfigs.setDao(daoConfig);

        service = new EvaluateSenderLimitJobServiceImpl(
                pnDelayerUtils,
                pnDelayerConfigs,
                new PaperDeliveryUtils(paperDeliveryDao, pnDelayerConfigs, pnDelayerUtils, deliveryDriverUtils, paperDeliveryCounterDAO),
                deliveryDriverUtils,
                ssmParameterConsumerActivation,
                new SenderLimitUtils(paperDeliverySenderLimitDAO, pnDelayerUtils, paperDeliveryCounterDAO)
        );

        Map<String, List<String>> priorityMap = Map.of(
                "1", List.of("PRODUCT_RS.ATTEMPT_0"),
                "2", List.of("PRODUCT_AR.ATTEMPT_1", "PRODUCT_890.ATTEMPT_1"),
                "3", List.of("PRODUCT_AR.ATTEMPT_0", "PRODUCT_890.ATTEMPT_0"));
        when(ssmParameterConsumerActivation.getParameterValue(eq("priority-param"), eq(Map.class)))
                .thenReturn(Optional.of(priorityMap));
    }

    @Test
    void startSenderLimitJob_singleDriver_withoutLastEvaluatedKey() {

        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("RS","AR"), 8, List.of("POSTE"));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(any(), eq(tenderId), eq(province)))
                .thenReturn(Mono.just(List.of(capacity)));

        List<PaperDelivery> deliveries = new ArrayList<>();
        deliveries.addAll(getPaperDeliveries(false));
        deliveries.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        when(page.lastEvaluatedKey()).thenReturn(new HashMap<>());
        when(paperDeliveryDao.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50)))
                .thenReturn(Mono.just(page));
        when(deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(anyList(), any(), any(), any()))
                .thenReturn(deliveries);

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("paId2~AR~RM");
        paperDeliverySenderLimit.setProductType("AR");
        paperDeliverySenderLimit.setWeeklyEstimate(1);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), any()))
                .thenReturn(Flux.empty());
        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), any()))
                .thenReturn(Flux.just(paperDeliverySenderLimit));
        when(paperDeliverySenderLimitDAO.updateUsedSenderLimit(anyString(), anyLong(), any(), anyInt()))
                .thenReturn(Mono.just(2L));

        ArgumentCaptor<List<PaperDelivery>> senderLimitJobPaperDeliveriesCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDao.insertPaperDeliveries(senderLimitJobPaperDeliveriesCaptor.capture()))
                .thenReturn(Mono.empty());

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setPk("SUM_ESTIMATES~AR~RM");
        paperDeliveryCounter.setNumberOfShipments(8);

        List<PaperDeliveryCounter> paperDeliveryCounterList = List.of(paperDeliveryCounter);

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(anyString(), anyString(), anyInt()))
                .thenReturn(Mono.just(paperDeliveryCounterList));

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, Instant.now()))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = senderLimitJobPaperDeliveriesCaptor.getAllValues();
        Assertions.assertEquals(3, capturedDeliveries.getFirst().size());
        Assertions.assertEquals(1, capturedDeliveries.getLast().size());
        verify(paperDeliverySenderLimitDAO, times(1)).updateUsedSenderLimit(any(), any(), any(), anyInt());
        verify(paperDeliveryDao, times(2)).insertPaperDeliveries(anyList());
        verify(paperDeliveryDao, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50));
    }

    @Test
    void startSenderLimitJob_singleDriver_withLastEvaluatedKeyAndWithoutSenderLimit() {

        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("RS", "AR"), 2, List.of("POSTE"));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(any(), eq(tenderId), eq(province)))
                .thenReturn(Mono.just(List.of(capacity)));

        List<PaperDelivery> deliveries = new ArrayList<>();
        deliveries.addAll(getPaperDeliveries(false));
        deliveries.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_SENDER_LIMIT).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("driver1~RM~2025-01-01T00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);;

        List<PaperDelivery> deliveries2 = new ArrayList<>();
        deliveries2.addAll(getPaperDeliveries(false));
        deliveries2.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(deliveries2);
        when(page2.lastEvaluatedKey()).thenReturn(new HashMap<>());

        when(paperDeliveryDao.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50)))
                .thenReturn(Mono.just(page))
                .thenReturn(Mono.just(page2));

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("paId2~AR~RM");
        paperDeliverySenderLimit.setProductType("AR");
        paperDeliverySenderLimit.setWeeklyEstimate(5);

        PaperDeliveryUsedSenderLimit usedSenderLimit = new PaperDeliveryUsedSenderLimit();
        usedSenderLimit.setPk("paId2~AR~RM");
        usedSenderLimit.setSenderLimit(5);
        usedSenderLimit.setNumberOfShipment(4);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), any()))
                .thenReturn(Flux.empty());
        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), any()))
                .thenReturn(Flux.empty());

        ArgumentCaptor<List<PaperDelivery>> senderLimitJobPaperDeliveriesCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDao.insertPaperDeliveries(senderLimitJobPaperDeliveriesCaptor.capture()))
                .thenReturn(Mono.empty());

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setNumberOfShipments(10);

        List<PaperDeliveryCounter> paperDeliveryCounterList = List.of(paperDeliveryCounter);

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(anyString(), anyString(), anyInt()))
                .thenReturn(Mono.just(paperDeliveryCounterList));
        when(deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(anyList(), any(), any(), any()))
                .thenReturn(deliveries)
                .thenReturn(deliveries2);

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, Instant.now()))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = senderLimitJobPaperDeliveriesCaptor.getAllValues();
        Assertions.assertEquals(2, capturedDeliveries.getFirst().size());
        Assertions.assertEquals(2, capturedDeliveries.get(1).size());
        Assertions.assertEquals(2, capturedDeliveries.get(2).size());
        Assertions.assertEquals(2, capturedDeliveries.getLast().size());
        verify(paperDeliverySenderLimitDAO, times(0)).updateUsedSenderLimit(any(), any(), any(), anyInt());
        verify(paperDeliveryDao, times(4)).insertPaperDeliveries(anyList());
        verify(paperDeliveryDao, times(2)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50));
    }

    @Test
    void startSenderLimitJob_singleDriver_withLastEvaluatedKey() {

        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("RS", "AR"), 2, List.of("POSTE"));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(any(), eq(tenderId), eq(province)))
                .thenReturn(Mono.just(List.of(capacity)));

        List<PaperDelivery> deliveries = new ArrayList<>();
        deliveries.addAll(getPaperDeliveries(false));
        deliveries.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_SENDER_LIMIT).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("driver1~RM~2025-01-01T00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);;

        List<PaperDelivery> deliveries2 = new ArrayList<>();
        deliveries2.addAll(getPaperDeliveries(false));
        deliveries2.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(deliveries2);
        when(page2.lastEvaluatedKey()).thenReturn(new HashMap<>());

        when(deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(anyList(), any(), any(), any()))
                .thenReturn(deliveries)
                .thenReturn(deliveries2);

        when(paperDeliveryDao.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50)))
                .thenReturn(Mono.just(page))
                .thenReturn(Mono.just(page2));

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("paId2~AR~RM");
        paperDeliverySenderLimit.setProductType("AR");
        paperDeliverySenderLimit.setWeeklyEstimate(5);

        PaperDeliveryUsedSenderLimit usedSenderLimit = new PaperDeliveryUsedSenderLimit();
        usedSenderLimit.setPk("paId2~AR~RM");
        usedSenderLimit.setSenderLimit(5);
        usedSenderLimit.setNumberOfShipment(4);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), any()))
                .thenReturn(Flux.empty())
                .thenReturn(Flux.just(usedSenderLimit));
        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), any()))
                .thenReturn(Flux.just(paperDeliverySenderLimit));
        when(paperDeliverySenderLimitDAO.updateUsedSenderLimit(anyString(), anyLong(), any(), anyInt()))
                .thenReturn(Mono.just(2L));

        ArgumentCaptor<List<PaperDelivery>> senderLimitJobPaperDeliveriesCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDao.insertPaperDeliveries(senderLimitJobPaperDeliveriesCaptor.capture()))
                .thenReturn(Mono.empty());

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setNumberOfShipments(10);

        List<PaperDeliveryCounter> paperDeliveryCounterList = List.of(paperDeliveryCounter);

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(anyString(), anyString(), anyInt()))
                .thenReturn(Mono.just(paperDeliveryCounterList));

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, Instant.now()))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = senderLimitJobPaperDeliveriesCaptor.getAllValues();
        Assertions.assertEquals(3, capturedDeliveries.getFirst().size());
        Assertions.assertEquals(1, capturedDeliveries.get(1).size());
        Assertions.assertEquals(3, capturedDeliveries.get(2).size());
        Assertions.assertEquals(1, capturedDeliveries.getLast().size());
        verify(paperDeliverySenderLimitDAO, times(2)).updateUsedSenderLimit(any(), any(), any(), anyInt());
        verify(paperDeliveryDao, times(4)).insertPaperDeliveries(anyList());
        verify(paperDeliveryDao, times(2)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50));
    }

    @Test
    void startSenderLimitJob_multipleDriver_withoutLastEvaluatedKey() {

        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("RS"),10, List.of("POSTE", "FULMINE"));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(any(), eq(tenderId), eq(province)))
                .thenReturn(Mono.just(List.of(capacity)));

        when(deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(anyList(), anyString()))
                .thenReturn(List.of(new PaperChannelDeliveryDriver("00184", "AR", "driver1"),
                        new PaperChannelDeliveryDriver("00185", "RS", "driver2")));

        List<PaperDelivery> deliveries = new ArrayList<>();
        deliveries.addAll(getPaperDeliveries(false));
        deliveries.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_RESIDUAL_CAPACITY).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("driver1~RM~2025-01-01T00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(new HashMap<>());
        when(paperDeliveryDao.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50)))
                .thenReturn(Mono.just(page));
        when(deliveryDriverUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(any(), any(), any())).thenReturn(deliveries);

        PaperDeliveryUsedSenderLimit usedSenderLimit = new PaperDeliveryUsedSenderLimit();
        usedSenderLimit.setPk("paId2~AR~RM");
        usedSenderLimit.setSenderLimit(5);
        usedSenderLimit.setNumberOfShipment(4);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), any()))
                .thenReturn(Flux.just(usedSenderLimit));
        when(paperDeliverySenderLimitDAO.updateUsedSenderLimit(anyString(), anyLong(), any(), anyInt()))
                .thenReturn(Mono.just(2L));

        ArgumentCaptor<List<PaperDelivery>> senderLimitJobPaperDeliveriesCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDao.insertPaperDeliveries(senderLimitJobPaperDeliveriesCaptor.capture()))
                .thenReturn(Mono.empty());

        when(deliveryDriverUtils.retrieveFromCache("00184~AR"))
                .thenReturn(Optional.empty());
        when(deliveryDriverUtils.retrieveFromCache("00185~RS"))
                .thenReturn(Optional.empty());

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setNumberOfShipments(100);

        List<PaperDeliveryCounter> paperDeliveryCounterList = List.of(paperDeliveryCounter);

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(anyString(), anyString(), anyInt()))
                .thenReturn(Mono.just(paperDeliveryCounterList));

        StepVerifier.create(service.startSenderLimitJob(province, tenderId, Instant.now()))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = senderLimitJobPaperDeliveriesCaptor.getAllValues();
        Assertions.assertEquals(3, capturedDeliveries.getFirst().size());
        Assertions.assertEquals(1, capturedDeliveries.getLast().size());
        verify(deliveryDriverUtils, times(2)).retrieveFromCache(anyString());
        verify(deliveryDriverUtils, times(1)).insertInCache(anyList());
        verify(deliveryDriverUtils, times(1)).retrieveUnifiedDeliveryDriversFromPaperChannel(anyList(), anyString());
        verify(paperDeliverySenderLimitDAO, times(1)).updateUsedSenderLimit(any(), any(), any(), anyInt());
        verify(paperDeliveryDao, times(2)).insertPaperDeliveries(anyList());
        verify(paperDeliveryDao, times(1)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50));
    }

    @Test
    void startSenderLimitJob_multiple_withLastEvaluatedKey() {

        DriversTotalCapacity capacity = new DriversTotalCapacity(List.of("RS", "AR"), 10, List.of("POSTE", "FULMINE"));
        when(deliveryDriverUtils.retrieveDriversCapacityOnProvince(any(), eq(tenderId), eq(province)))
                .thenReturn(Mono.just(List.of(capacity)));

        when(deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(anyList(), anyString()))
                .thenReturn(List.of(new PaperChannelDeliveryDriver("00184", "AR", "driver1"),
                        new PaperChannelDeliveryDriver("00184", "RS", "driver2")))
                .thenReturn(List.of(new PaperChannelDeliveryDriver("00185", "RS", "driver2")));


        List<PaperDelivery> deliveries = new ArrayList<>();
        deliveries.addAll(getPaperDeliveries(true));
        deliveries.addAll(getPaperDeliveries(true));
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(deliveries);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("pk", AttributeValue.builder().s("2025-01-01~" + EVALUATE_SENDER_LIMIT).build());
        lastEvaluatedKey.put("sk", AttributeValue.builder().s("driver1~RM~2025-01-01T00:00:00Z~requestId2").build());
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);;

        List<PaperDelivery> deliveries2 = new ArrayList<>();
        deliveries2.addAll(getPaperDeliveries(true));
        deliveries2.addAll(getPaperDeliveries(false));
        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(deliveries2);
        when(page2.lastEvaluatedKey()).thenReturn(new HashMap<>());

        when(paperDeliveryDao.retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50)))
                .thenReturn(Mono.just(page))
                .thenReturn(Mono.just(page2));

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("paId2~AR~RM");
        paperDeliverySenderLimit.setProductType("AR");
        paperDeliverySenderLimit.setWeeklyEstimate(5);

        PaperDeliveryUsedSenderLimit usedSenderLimit = new PaperDeliveryUsedSenderLimit();
        usedSenderLimit.setPk("paId2~AR~RM");
        usedSenderLimit.setSenderLimit(5);
        usedSenderLimit.setNumberOfShipment(4);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), any()))
                .thenReturn(Flux.empty())
                .thenReturn(Flux.just(usedSenderLimit));
        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), any()))
                .thenReturn(Flux.just(paperDeliverySenderLimit));
        when(paperDeliverySenderLimitDAO.updateUsedSenderLimit(anyString(), anyLong(), any(), anyInt()))
                .thenReturn(Mono.just(2L));

        ArgumentCaptor<List<PaperDelivery>> senderLimitJobPaperDeliveriesCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDao.insertPaperDeliveries(senderLimitJobPaperDeliveriesCaptor.capture()))
                .thenReturn(Mono.empty());

        PaperDeliveryCounter paperDeliveryCounter1 = new PaperDeliveryCounter();
        paperDeliveryCounter1.setNumberOfShipments(10);
        paperDeliveryCounter1.setPk("SUM_ESTIMATES~AR~RM");

        when(deliveryDriverUtils.retrieveFromCache("00184~AR"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of("driver1"));
        when(deliveryDriverUtils.retrieveFromCache("00184~RS"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of("driver2"));
        when(deliveryDriverUtils.retrieveFromCache("00185~RS"))
                .thenReturn(Optional.empty());
        when(deliveryDriverUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(any(), any(), any())).thenReturn(deliveries2);
        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(anyString(), anyString(), anyInt()))
                .thenReturn(Mono.just(List.of(paperDeliveryCounter1)));


        StepVerifier.create(service.startSenderLimitJob(province, tenderId, Instant.now()))
                .verifyComplete();

        List<List<PaperDelivery>> capturedDeliveries = senderLimitJobPaperDeliveriesCaptor.getAllValues();
        Assertions.assertEquals(4, capturedDeliveries.getFirst().size());
        Assertions.assertEquals(0, capturedDeliveries.get(1).size());
        Assertions.assertEquals(3, capturedDeliveries.get(2).size());
        Assertions.assertEquals(1, capturedDeliveries.getLast().size());
        verify(paperDeliverySenderLimitDAO, times(2)).updateUsedSenderLimit(any(), any(), any(), anyInt());
        verify(paperDeliveryDao, times(4)).insertPaperDeliveries(anyList());
        verify(paperDeliveryDao, times(2)).retrievePaperDeliveries(eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT), any(), any(), any(), eq(50));
        verify(deliveryDriverUtils, times(5)).retrieveFromCache(anyString());
        verify(deliveryDriverUtils, times(2)).insertInCache(anyList());
        verify(deliveryDriverUtils, times(2)).retrieveUnifiedDeliveryDriversFromPaperChannel(anyList(), anyString());

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
        paperDelivery1.setSenderPaId("paId1");
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
        paperDelivery2.setSenderPaId("paId2");
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

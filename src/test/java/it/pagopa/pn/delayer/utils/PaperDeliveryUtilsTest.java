package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.SenderLimitJobProcessObjects;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
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
import java.time.LocalDate;
import java.util.*;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaperDeliveryUtilsTest {

    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private DeliveryDriverUtils deliveryDriverUtils;

    @Mock
    private PaperDeliveryDAO paperDeliveryDAO;

    @Mock
    private PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs config = new PnDelayerConfigs();
        PnDelayerConfigs.Dao daoConfig = new PnDelayerConfigs.Dao();
        daoConfig.setPaperDeliveryQueryLimit(10);
        config.setDao(daoConfig);
        config.setPrintCapacityWeeklyWorkingDays(7);
        config.setPrintCapacity(List.of("1970-01-01;180000"));
        config.setPrintCounterTtlDuration(Duration.ofDays(7));
        config.setDeliveryDateDayOfWeek(1);
        paperDeliveryUtils = new PaperDeliveryUtils(paperDeliveryDAO, config, new PnDelayerUtils(config, new PrintCapacityUtils(config)), deliveryDriverUtils, paperDeliveryCounterDAO);
    }


    void evaluateCapacitiesAndProcessDeliveries() {
        Tuple2<Integer, Integer> provinceCapacities = Tuples.of(10,5);
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        LocalDate deliveryWeek = LocalDate.now();
        String tenderId = "tender1";
        when(deliveryDriverUtils.updateCounters(anyList())).thenReturn(Mono.empty());
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(anyString(), anyString(), anyString(), any()))
                .thenReturn(Mono.just(provinceCapacities));

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(List.of(createPaperDelivery("AR", "00179", province, "senderPaId1", 1),
                        createPaperDelivery("AR", "00178", province, "senderPaId2", 1)))));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(workflowStepEnum, unifiedDeliveryDriver, province, deliveryWeek, tenderId))
                .verifyComplete();
    }

    void evaluateCapacitiesAndProcessDeliveriesNoCapacityOnProvince() {
        Tuple2<Integer, Integer> provinceCapacities = Tuples.of(10,10);
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        LocalDate deliveryWeek = LocalDate.now();
        String tenderId = "tender1";
        when(deliveryDriverUtils.updateCounters(anyList())).thenReturn(Mono.empty());
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(anyString(), anyString(), anyString(), any()))
                .thenReturn(Mono.just(provinceCapacities));

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(List.of(createPaperDelivery("AR", "00179", province, "senderPaId1", 1),
                        createPaperDelivery("AR", "00178", province, "senderPaId2", 1)))));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(workflowStepEnum, unifiedDeliveryDriver, province, deliveryWeek, tenderId))
                .verifyComplete();
    }


    @Test
    void retrievePaperDeliveries_handlesNoRecordsFound() {
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        LocalDate deliveryWeek = LocalDate.now();
        String sortKeyPrefix = "driver1~RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        int queryLimit = 10;

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(List.of())));

        StepVerifier.create(paperDeliveryUtils.retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, queryLimit))
                .verifyComplete();
    }

    @Test
    void insertPaperDeliveries(){
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(new PaperDelivery());
        LocalDate deliveryWeek = LocalDate.now();

        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();
        senderLimitJobProcessObjects.setSendToDriverCapacityStep(List.of(createPaperDelivery("AR", "00100", "RM", "paId", 1)));
        senderLimitJobProcessObjects.setSendToResidualCapacityStep(List.of(createPaperDelivery("AR", "00100", "RM", "paId", 0)));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        paperDeliveryUtils.insertPaperDeliveries(new SenderLimitJobProcessObjects(), deliveryWeek).block();

        verify(paperDeliveryDAO,times(2)).insertPaperDeliveries(anyList());
    }

    @Test
    void evaluateCapacitiesNoCapacity_sendToNextWeek_multiPage() {
        // Province capacity exhausted → all items go to next week via sendToNextWeek.
        // DynamoDB returns 3 pages: page1 → page2 → page3 (last page, no lastEvaluatedKey).
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        LocalDate deliveryWeek = LocalDate.parse("2025-01-06");
        String tenderId = "tender1";

        // Province capacity = 0 → triggers sendToNextWeek path
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(eq(province), any(), any(), any()))
                .thenReturn(Mono.just(Tuples.of(10, 10)));

        // Build 3 pages with lastEvaluatedKey chaining
        Map<String, AttributeValue> key1 = Map.of("pk", AttributeValue.builder().s("k1").build());
        Map<String, AttributeValue> key2 = Map.of("pk", AttributeValue.builder().s("k2").build());

        Page<PaperDelivery> page1 = mock(Page.class);
        when(page1.items()).thenReturn(List.of(
                createPaperDelivery("AR", "00179", province, "sender1", 1),
                createPaperDelivery("AR", "00178", province, "sender2", 1)));
        when(page1.lastEvaluatedKey()).thenReturn(key1);

        Page<PaperDelivery> page2 = mock(Page.class);
        when(page2.items()).thenReturn(List.of(
                createPaperDelivery("RS", "00180", province, "sender3", 0)));
        when(page2.lastEvaluatedKey()).thenReturn(key2);

        // Last page — empty lastEvaluatedKey signals end of pagination
        Page<PaperDelivery> page3 = mock(Page.class);
        when(page3.items()).thenReturn(List.of(
                createPaperDelivery("AR", "00181", province, "sender4", 1)));
        when(page3.lastEvaluatedKey()).thenReturn(Collections.emptyMap());

        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                argThat(map -> map != null && map.isEmpty()), eq(10)))
                .thenReturn(Mono.just(page1));

        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                eq(key1), eq(10)))
                .thenReturn(Mono.just(page2));

        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                eq(key2), eq(10)))
                .thenReturn(Mono.just(page3));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(
                        WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, unifiedDeliveryDriver, province, deliveryWeek, tenderId))
                .verifyComplete();

        // retrievePaperDeliveries called 3 times (one per page)
        verify(paperDeliveryDAO, times(3)).retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(), eq(10));

        // insertPaperDeliveries called 3 times (one insert per page chunk)
        verify(paperDeliveryDAO, times(3)).insertPaperDeliveries(anyList());
    }

    @Test
    void evaluateCapacitiesWithCapacity_sendToNextStep_multiPage() {
        // Province has capacity → items go through sendToNextStep with multi-page pagination.
        // After capacity is consumed, remaining items go to next week.
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        LocalDate deliveryWeek = LocalDate.parse("2025-01-06");
        String tenderId = "tender1";

        // Province: declared=3, used=0 → residual=3
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(eq(province), any(), any(), any()))
                .thenReturn(Mono.just(Tuples.of(3, 0)));

        // CAP capacity: enough for all items
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(eq("00184"), any(), any(), any()))
                .thenReturn(Mono.just(Tuples.of(5, 0)))
                .thenReturn(Mono.just(Tuples.of(5, 0)));
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(eq("00185"), any(), any(), any()))
                .thenReturn(Mono.just(Tuples.of(1, 1)));

        // Page 1: 2 items, has more pages
        Map<String, AttributeValue> key1 = Map.of("pk", AttributeValue.builder().s("k1").build());
        Page<PaperDelivery> page1 = mock(Page.class);
        PaperDelivery d1 = createPaperDelivery("AR", "00185", province, "sender1", 0);
        d1.setPk("2025-01-06~" + EVALUATE_DRIVER_CAPACITY);
        d1.setSk("driver1~RM~2025-01-02T00:00:00Z~req1");
        d1.setPrepareRequestDate("2025-01-02T00:00:00Z");
        d1.setCreatedAt("2025-01-01T00:00:00Z");
        PaperDelivery d2 = createPaperDelivery("AR", "00184", province, "sender2", 0);
        d2.setPk("2025-01-06~" + EVALUATE_DRIVER_CAPACITY);
        d2.setSk("driver1~RM~2025-01-01T00:00:00Z~req2");
        d2.setPrepareRequestDate("2025-01-02T00:00:00Z");
        d2.setCreatedAt("2025-01-01T00:00:00Z");
        when(page1.items()).thenReturn(List.of(d1, d2));
        when(page1.lastEvaluatedKey()).thenReturn(key1);

        // Page 2: 2 more items, has more pages
        Map<String, AttributeValue> key2 = Map.of("pk", AttributeValue.builder().s("k2").build());
        Page<PaperDelivery> page2 = mock(Page.class);
        PaperDelivery d3 = createPaperDelivery("AR", "00184", province, "sender3", 0);
        d3.setPk("2025-01-06~" + EVALUATE_DRIVER_CAPACITY);
        d3.setSk("driver1~RM~2025-01-01T00:00:00Z~req3");
        d3.setPrepareRequestDate("2025-01-02T00:00:00Z");
        d3.setCreatedAt("2025-01-01T00:00:00Z");
        PaperDelivery d4 = createPaperDelivery("AR", "00184", province, "sender4", 0);
        d4.setPk("2025-01-06~" + EVALUATE_DRIVER_CAPACITY);
        d4.setSk("driver1~RM~2025-01-01T00:00:00Z~req4");
        d4.setPrepareRequestDate("2025-01-02T00:00:00Z");
        d4.setCreatedAt("2025-01-01T00:00:00Z");
        when(page2.items()).thenReturn(List.of(d3, d4));
        when(page2.lastEvaluatedKey()).thenReturn(key2);

        // Remaining page for sendToNextWeek (after capacity exhausted)
        Page<PaperDelivery> lastPage = mock(Page.class);
        PaperDelivery d5 = createPaperDelivery("AR", "00184", province, "sender5", 0);
        d5.setPk("2025-01-06~" + EVALUATE_DRIVER_CAPACITY);
        d5.setSk("driver1~RM~2025-01-01T00:00:00Z~req5");
        d5.setPrepareRequestDate("2025-01-02T00:00:00Z");
        d5.setCreatedAt("2025-01-01T00:00:00Z");
        when(lastPage.items()).thenReturn(List.of(d5));
        when(lastPage.lastEvaluatedKey()).thenReturn(Collections.emptyMap());

        // Page 1 returned for queryLimit=min(3,10)=3
        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                argThat(map -> map != null && map.isEmpty()), eq(3)))
                .thenReturn(Mono.just(page1));

        // Page 2 returned for queryLimit=min(2,10)=2 (residual shrinks after page1)
        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                eq(key1), anyInt()))
                .thenReturn(Mono.just(page2));

        // Last page for sendToNextWeek
        when(paperDeliveryDAO.retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                eq(key2), eq(10)))
                .thenReturn(Mono.just(lastPage));

        ArgumentCaptor<List<PaperDelivery>> insertCaptor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryDAO.insertPaperDeliveries(insertCaptor.capture())).thenReturn(Mono.empty());
        when(paperDeliveryCounterDAO.updatePrintCapacityCounter(any(), anyInt(), anyInt())).thenReturn(Mono.empty());
        when(deliveryDriverUtils.updateCounters(anyList())).thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(
                        WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, unifiedDeliveryDriver, province, deliveryWeek, tenderId))
                .verifyComplete();

        // At least 2 retrievePaperDeliveries calls for sendToNextStep pagination + 1 for sendToNextWeek
        verify(paperDeliveryDAO, atLeast(2)).retrievePaperDeliveries(
                eq(EVALUATE_DRIVER_CAPACITY), any(),
                eq(String.join("~", unifiedDeliveryDriver, province)),
                any(), anyInt());

        // insertPaperDeliveries called multiple times (chunks from nextStep + nextWeek)
        verify(paperDeliveryDAO, atLeast(2)).insertPaperDeliveries(anyList());

        // Counters flushed exactly once
        verify(deliveryDriverUtils, times(1)).updateCounters(anyList());
    }

    private PaperDelivery createPaperDelivery(String productType, String cap, String province, String senderPaId, Integer attempt) {
        PaperDelivery delivery = new PaperDelivery();
        delivery.setCap(cap);
        delivery.setProvince(province);
        delivery.setProductType(productType);
        delivery.setUnifiedDeliveryDriver("driver1");
        delivery.setRequestId("requestId");
        delivery.setNotificationSentAt("2023-10-01T12:00:00Z");
        delivery.setSenderPaId(senderPaId);
        delivery.setDeliveryDate("2023-10-02");
        delivery.setPriority(1);
        delivery.setAttempt(attempt);
        delivery.setDeliveryDate("2023-10-02");
        delivery.setPk("2023-10-02~EVALUATE_RESIDUAL_CAPACITY");
        return delivery;
    }
}

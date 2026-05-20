package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EvaluateSenderPriorityJobServiceTest {

    private EvaluateSenderPriorityJobServiceImpl service;

    @Mock
    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    private final LocalDate deliveryWeek = LocalDate.of(2025, 1, 6);
    private final String senderPaId = "senderPaId";

    @BeforeEach
    void setup() {
        PnDelayerConfigs.Dao dao = new PnDelayerConfigs.Dao();
        dao.setPaperDeliveryQueryLimit(50);
        when(pnDelayerConfigs.getDao()).thenReturn(dao);
        service = new EvaluateSenderPriorityJobServiceImpl(paperDeliveryUtils, pnDelayerConfigs);
    }

    @Test
    void startSenderPriorityJob_withoutDeliveries_shouldCompleteWithoutWriting() {
        when(paperDeliveryUtils.retrievePaperDeliveriesToReorder(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                eq(deliveryWeek),
                eq(senderPaId+"~"),
                isNull(),
                eq(50), anyString()
        )).thenReturn(Mono.empty());

        StepVerifier.create(service.startSenderPriorityJob(senderPaId, deliveryWeek))
                .verifyComplete();

        verify(paperDeliveryUtils, never()).transactReorderedDeliveries(anyList());
    }

    @Test
    void startSenderPriorityJob_singlePage_shouldReorderBySenderPriorityUsingOriginalDates() {
        PaperDelivery lowPriority = delivery(
                "req-low",
                0,
                "RM~2025-01-03T10:00:00Z~req-low",
                "2025-01-03T10:00:00Z",
                "RM"
        );
        PaperDelivery mediumPriority = delivery(
                "req-medium",
                50,
                "MI~2025-01-05T10:02:00Z~req-medium",
                "2025-01-05T10:02:00Z",
                "MI"
        );
        PaperDelivery highPriority = delivery(
                "req-high",
                100,
                "MI~2025-01-06T10:01:00Z~req-high",
                "2025-01-06T10:01:00Z",
                "MI"
        );

        Page<PaperDelivery> page = page(
                List.of(lowPriority, mediumPriority, highPriority),
                Collections.emptyMap()
        );

        whenRetrieve(null).thenReturn(Mono.just(page));

        ArgumentCaptor<List<PaperDelivery>> captor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryUtils.transactReorderedDeliveries(captor.capture()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.startSenderPriorityJob(senderPaId, deliveryWeek))
                .verifyComplete();

        List<PaperDelivery> reordered = captor.getValue();

        assertEquals(3, reordered.size());

        assertEquals("req-high", reordered.getFirst().getRequestId());
        assertEquals("MI~2025-01-03T10:00:00Z~req-high", reordered.getFirst().getSk());
        assertEquals("MI~2025-01-06T10:01:00Z~req-high", reordered.getFirst().getOldSk());
        assertEquals("2025-01-03T10:00:00Z", reordered.getFirst().getVirtualNotificationSentAt());

        assertEquals("req-medium", reordered.get(1).getRequestId());
        assertEquals("MI~2025-01-05T10:02:00Z~req-medium", reordered.get(1).getSk());
        assertEquals("MI~2025-01-05T10:02:00Z~req-medium", reordered.get(1).getOldSk());
        assertEquals("2025-01-05T10:02:00Z", reordered.get(1).getVirtualNotificationSentAt());

        assertEquals("req-low", reordered.getLast().getRequestId());
        assertEquals("RM~2025-01-06T10:01:00Z~req-low", reordered.getLast().getSk());
        assertEquals("RM~2025-01-03T10:00:00Z~req-low", reordered.getLast().getOldSk());
        assertEquals("2025-01-06T10:01:00Z", reordered.getLast().getVirtualNotificationSentAt());

        verify(paperDeliveryUtils, times(1)).transactReorderedDeliveries(anyList());
    }

    @Test
    void startSenderPriorityJob_multiplePages_shouldRetrieveUntilLastEvaluatedKeyIsEmpty() {
        Map<String, AttributeValue> lastEvaluatedKey = Map.of(
                "pk", AttributeValue.builder().s(deliveryWeek + "~EVALUATE_SENDER_LIMIT").build(),
                "sk", AttributeValue.builder().s(senderPaId + "~2025-01-06T10:01:00Z~req-2").build()
        );

        Page<PaperDelivery> firstPage = page(
                List.of(
                        delivery(   "req-1",
                                90,
                                "MI~2025-01-06T10:01:00Z~req-1",
                                "2025-01-06T10:01:00Z",
                                "MI"),
                        delivery(   "req-2",
                                30,
                                "MI~2025-01-07T10:01:00Z~req-2",
                                "2025-01-07T10:01:00Z",
                                "MI")
                ),
                lastEvaluatedKey
        );

        Page<PaperDelivery> secondPage = page(
                List.of(
                        delivery("req-3",
                                25,
                                "MI~2025-01-08T10:01:00Z~req-3",
                                "2025-01-08T10:01:00Z",
                                "MI"),
                        delivery("req-4",
                                100,
                                "MI~2025-01-08T10:02:00Z~req-4",
                                "2025-01-08T10:02:00Z",
                                "MI")
                ),
                Collections.emptyMap()
        );

        whenRetrieve(null).thenReturn(Mono.just(firstPage));
        whenRetrieve(lastEvaluatedKey).thenReturn(Mono.just(secondPage));

        ArgumentCaptor<List<PaperDelivery>> captor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryUtils.transactReorderedDeliveries(captor.capture()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.startSenderPriorityJob(senderPaId, deliveryWeek))
                .verifyComplete();

        List<PaperDelivery> reordered = captor.getValue();

        assertEquals("req-4", reordered.getFirst().getRequestId());
        assertEquals("MI~2025-01-06T10:01:00Z~req-4", reordered.getFirst().getSk());
        assertEquals("MI~2025-01-08T10:02:00Z~req-4", reordered.getFirst().getOldSk());
        assertEquals("2025-01-06T10:01:00Z", reordered.getFirst().getVirtualNotificationSentAt());

        assertEquals("req-1", reordered.get(1).getRequestId());
        assertEquals("MI~2025-01-07T10:01:00Z~req-1", reordered.get(1).getSk());
        assertEquals("MI~2025-01-06T10:01:00Z~req-1", reordered.get(1).getOldSk());
        assertEquals("2025-01-07T10:01:00Z", reordered.get(1).getVirtualNotificationSentAt());

        assertEquals("req-2", reordered.get(2).getRequestId());
        assertEquals("MI~2025-01-08T10:01:00Z~req-2", reordered.get(2).getSk());
        assertEquals("MI~2025-01-07T10:01:00Z~req-2", reordered.get(2).getOldSk());
        assertEquals("2025-01-08T10:01:00Z", reordered.get(2).getVirtualNotificationSentAt());

        assertEquals("req-3", reordered.getLast().getRequestId());
        assertEquals("MI~2025-01-08T10:02:00Z~req-3", reordered.getLast().getSk());
        assertEquals("MI~2025-01-08T10:01:00Z~req-3", reordered.getLast().getOldSk());
        assertEquals("2025-01-08T10:02:00Z", reordered.getLast().getVirtualNotificationSentAt());

        verify(paperDeliveryUtils, times(1)).retrievePaperDeliveriesToReorder(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                eq(deliveryWeek),
                eq(senderPaId+"~"),
                isNull(),
                eq(50),
                eq(PaperDelivery.PK_SENDERPAID_ORIGINALSENTAT_INDEX)
        );

        verify(paperDeliveryUtils, times(1)).retrievePaperDeliveriesToReorder(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                eq(deliveryWeek),
                eq(senderPaId+"~"),
                eq(lastEvaluatedKey),
                eq(50),
                eq(PaperDelivery.PK_SENDERPAID_ORIGINALSENTAT_INDEX)
        );
    }

   @Test
    void startSenderPriorityJob_samePriority_shouldKeepOriginalRelativeOrder() {
        PaperDelivery first = delivery("req-1",
                25,
                "MI~2025-01-08T10:01:00Z~req-1",
                "2025-01-08T10:01:00Z",
                "MI");
        PaperDelivery second = delivery("req-2",
                25,
                "MI~2025-01-08T10:02:00Z~req-2",
                "2025-01-08T10:02:00Z",
                "MI");
        PaperDelivery third = delivery("req-3",
                25,
                "MI~2025-01-08T10:03:00Z~req-3",
                "2025-01-08T10:03:00Z",
                "MI");

       Page<PaperDelivery> page = page(List.of(first, second, third),
               Collections.emptyMap()
       );

        whenRetrieve(null).thenReturn(Mono.just(page));

        ArgumentCaptor<List<PaperDelivery>> captor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryUtils.transactReorderedDeliveries(captor.capture()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.startSenderPriorityJob(senderPaId, deliveryWeek))
                .verifyComplete();

        List<PaperDelivery> reordered = captor.getValue();

        assertEquals("req-1", reordered.get(0).getRequestId());
        assertEquals("req-2", reordered.get(1).getRequestId());
        assertEquals("req-3", reordered.get(2).getRequestId());
    }

    @Test
    void startSenderPriorityJob_moreThanFiftyDeliveries_shouldWriteInBatchesOfFifty() {
        List<PaperDelivery> deliveries = new ArrayList<>();
        for (int i = 0; i < 51; i++) {
            deliveries.add(delivery(
                    "req-" + i,
                    i % 5,
                    String.format("RM~2025-01-06T10:%02d:00Z~req-%02d", i, i),
                    "2025-01-06T10:%02d:00Z".formatted(i),
                    "RM"
            ));
        }

        Page<PaperDelivery> page = page(deliveries, Collections.emptyMap());

        whenRetrieve(null).thenReturn(Mono.just(page));

        ArgumentCaptor<List<PaperDelivery>> captor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryUtils.transactReorderedDeliveries(captor.capture()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.startSenderPriorityJob(senderPaId, deliveryWeek))
                .verifyComplete();

        List<List<PaperDelivery>> batches = captor.getAllValues();

        assertEquals(2, batches.size());
        assertEquals(50, batches.get(0).size());
        assertEquals(1, batches.get(1).size());

        verify(paperDeliveryUtils, times(2)).transactReorderedDeliveries(anyList());
    }

    @Test
    void startSenderPriorityJob_alreadyReordered_shouldUseOriginalDatesFromSenderPaIdSentAt() {
        PaperDelivery lowPriority = delivery(
                "req-low",
                0,
                "RM~2025-01-06T10:01:00Z~req-low",
                "2025-01-03T10:00:00Z",
                "RM"
        );
        PaperDelivery mediumPriority = delivery(
                "req-medium",
                50,
                "MI~2025-01-05T10:02:00Z~req-medium",
                "2025-01-05T10:02:00Z",
                "MI"
        );
        PaperDelivery highPriority = delivery(
                "req-high",
                100,
                "MI~2025-01-03T10:00:00Z~req-high",
                "2025-01-06T10:01:00Z",
                "MI"
        );

        Page<PaperDelivery> page = page(
                List.of(lowPriority, mediumPriority, highPriority),
                Collections.emptyMap()
        );

        whenRetrieve(null).thenReturn(Mono.just(page));

        ArgumentCaptor<List<PaperDelivery>> captor = ArgumentCaptor.forClass(List.class);
        when(paperDeliveryUtils.transactReorderedDeliveries(captor.capture()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.startSenderPriorityJob(senderPaId, deliveryWeek))
                .verifyComplete();

        List<PaperDelivery> reordered = captor.getValue();

        assertEquals("req-high", reordered.getFirst().getRequestId());
        assertEquals("MI~2025-01-03T10:00:00Z~req-high", reordered.getFirst().getSk());

        assertEquals("req-medium", reordered.get(1).getRequestId());
        assertEquals("MI~2025-01-05T10:02:00Z~req-medium", reordered.get(1).getSk());

        assertEquals("req-low", reordered.getLast().getRequestId());
        assertEquals("RM~2025-01-06T10:01:00Z~req-low", reordered.getLast().getSk());
    }

    private org.mockito.stubbing.OngoingStubbing<Mono<Page<PaperDelivery>>> whenRetrieve(Map<String, AttributeValue> lastEvaluatedKey) {
        return when(paperDeliveryUtils.retrievePaperDeliveriesToReorder(
                eq(WorkflowStepEnum.EVALUATE_SENDER_LIMIT),
                eq(deliveryWeek),
                eq(senderPaId+"~"),
                lastEvaluatedKey == null ? isNull() : eq(lastEvaluatedKey),
                eq(50),
                eq(PaperDelivery.PK_SENDERPAID_ORIGINALSENTAT_INDEX)
        ));
    }

    private static Page<PaperDelivery> page(List<PaperDelivery> items, Map<String, AttributeValue> lastEvaluatedKey) {
        Page<PaperDelivery> page = mock(Page.class);
        when(page.items()).thenReturn(items);
        when(page.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);
        return page;
    }

    private PaperDelivery delivery(String requestId, Integer senderPriority, String sk, String sentAt, String province) {
        PaperDelivery paperDelivery = new PaperDelivery();
        paperDelivery.setPk(deliveryWeek + "~EVALUATE_SENDER_LIMIT");
        paperDelivery.setSk(sk);
        paperDelivery.setRequestId(requestId);
        paperDelivery.setSenderPaId(senderPaId);
        paperDelivery.setSenderPaIdOriginalSentAt(senderPaId + "~" + sentAt);
        paperDelivery.setSenderPriority(senderPriority);
        paperDelivery.setProvince(province);
        paperDelivery.setDeliveryDate(deliveryWeek.toString());
        paperDelivery.setProductType("AR");
        paperDelivery.setAttempt(0);
        return paperDelivery;
    }
}
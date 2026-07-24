package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.SenderLimitJobProcessObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SenderLimitUtilsTest {

    private SenderLimitUtils senderLimitUtils;

    @Mock
    private PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @Mock
    private PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        senderLimitUtils = new SenderLimitUtils(paperDeliverySenderLimitDAO, new PnDelayerUtils(pnDelayerConfigs, new PrintCapacityUtils(pnDelayerConfigs)), paperDeliveryCounterDAO);
    }

    @Test
    void retrieveAndEvaluateSenderLimit() {
        LocalDate deliveryWeek = LocalDate.now();
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId =
                Map.of("paid1~productType1~province1", List.of(createPaperDelivery("product1", "cap1", "province1", "senderPaId1", 1)),
                        "paid2~productType2~province2", List.of(createPaperDelivery("product2", "cap2", "province2", "senderPaId2", 1)));
        Map<String, Tuple2<Integer, Integer>> senderLimitMaps = new HashMap<>();
        Integer capacity = 72;
        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();
        senderLimitJobProcessObjects.setSenderLimitMap(senderLimitMaps);
        senderLimitJobProcessObjects.setTotalEstimateCounter(Map.of("AR", 72));

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("key2");
        paperDeliverySenderLimit.setProductType("AR");
        paperDeliverySenderLimit.setWeeklyEstimate(62);

        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), eq(deliveryWeek.minusWeeks(1))))
                .thenReturn(Flux.just(paperDeliverySenderLimit));

        senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, List.of(new DriversTotalCapacity(List.of("RS", "AR"), capacity, List.of("POSTE"))), senderLimitJobProcessObjects)
                .block();

        assertEquals(1, senderLimitMaps.size());
        assertTrue(senderLimitMaps.containsKey("key2"));
        assertEquals(62, senderLimitMaps.get("key2").getT1());
        assertEquals(0, senderLimitMaps.get("key2").getT2());
    }

    @Test
    void retrieveAndEvaluateSenderLimitErrorInPercentage() {
        LocalDate deliveryWeek = LocalDate.now();
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId =
                Map.of("paid1~productType1~province1", List.of(createPaperDelivery("product1", "cap1", "province1", "senderPaId1", 1)),
                        "paid2~productType2~province2", List.of(createPaperDelivery("product2", "cap2", "province2", "senderPaId2", 1)));
        Map<String, Tuple2<Integer, Integer>> senderLimitMaps = new HashMap<>();
        Integer capacity = 72;
        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();
        senderLimitJobProcessObjects.setSenderLimitMap(senderLimitMaps);
        senderLimitJobProcessObjects.setTotalEstimateCounter(Map.of("AR", 72));

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("key2");
        paperDeliverySenderLimit.setProductType("AR");
        paperDeliverySenderLimit.setPaId("paid1");
        paperDeliverySenderLimit.setProvince("province1");
        paperDeliverySenderLimit.setWeeklyEstimate(100);

        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), eq(deliveryWeek.minusWeeks(1))))
                .thenReturn(Flux.just(paperDeliverySenderLimit));

        Assertions.assertThrows(PnInternalException.class, () -> senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, List.of(new DriversTotalCapacity(List.of("RS", "AR"), capacity, List.of("POSTE"))), senderLimitJobProcessObjects)
                .block(), "Sender limit percentage exceeds 100%% for productType=AR, paId=paid1, province=province1");
    }

    @Test
    void createIncrementUsedSenderLimitDtos() {
        Map<String, Tuple2<Integer, Integer>> senderLimitMaps = Map.of("paId~AR~RM", Tuples.of(100, 50));

        StepVerifier.create(senderLimitUtils.createIncrementUsedSenderLimitDtos(senderLimitMaps))
                .expectNextMatches(incrementUsedSenderLimitDto -> {
                    assertEquals("paId~AR~RM", incrementUsedSenderLimitDto.pk());
                    assertEquals(50, incrementUsedSenderLimitDto.increment());
                    assertEquals(100, incrementUsedSenderLimitDto.senderLimit());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    void getResidualCapacityReturnsEmptyMapWhenCountersAreEmpty() {
        StepVerifier.create(senderLimitUtils.getResidualCapacity(List.of()))
                .assertNext(residualCapacity -> assertTrue(residualCapacity.isEmpty()))
                .verifyComplete();
    }

    @Test
    void getResidualCapacityCalculatesResidualForSingleCounter() {
        String notificationWeek = "2025-01-06";
        PaperDeliveryCounter counter = createCounter(notificationWeek, "DELAYED~RM~AR~PA1~2025-01-06", 10);
        String pk = "PA1~AR~RM";

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(eq(List.of(pk)), eq(LocalDate.parse(notificationWeek))))
                .thenReturn(Flux.just(createUsedSenderLimit(pk, 4)));

        StepVerifier.create(senderLimitUtils.getResidualCapacity(List.of(counter)))
                .assertNext(residualCapacity -> {
                    assertEquals(1, residualCapacity.size());
                    assertEquals(6, residualCapacity.get("2025-01-06~PA1~AR~RM"));
                })
                .verifyComplete();
    }

    @Test
    void getResidualCapacityAggregatesCountersFromDifferentNotificationWeeks() {
        PaperDeliveryCounter weekOneCounter = createCounter("2025-01-06", "DELAYED~RM~AR~PA1~2025-01-06", 12);
        PaperDeliveryCounter weekTwoCounter = createCounter("2025-01-13", "DELAYED~MI~890~PA2~2025-01-13", 8);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), eq(LocalDate.parse("2025-01-06"))))
                .thenReturn(Flux.just(createUsedSenderLimit("PA1~AR~RM", 5)));

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), eq(LocalDate.parse("2025-01-13"))))
                .thenReturn(Flux.just(createUsedSenderLimit("PA2~890~MI", 3)));

        StepVerifier.create(senderLimitUtils.getResidualCapacity(List.of(weekOneCounter, weekTwoCounter)))
                .assertNext(residualCapacity -> {
                    assertEquals(2, residualCapacity.size());
                    assertEquals(7, residualCapacity.get("2025-01-06~PA1~AR~RM"));
                    assertEquals(5, residualCapacity.get("2025-01-13~PA2~890~MI"));
                })
                .verifyComplete();
    }

    @Test
    void getResidualCapacityUsesZeroWhenWeeklyEstimateOrUsedShipmentsAreMissing() {
        String notificationWeek = "2025-01-20";
        PaperDeliveryCounter counterWithNullWeeklyEstimate = createCounter(notificationWeek, "DELAYED~RM~AR~PA1~2025-01-20", null);
        PaperDeliveryCounter counterWithUsedShipmentNull = createCounter(notificationWeek, "DELAYED~RM~AR~PA2~2025-01-20", 5);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), eq(LocalDate.parse(notificationWeek))))
                .thenReturn(Flux.just(createUsedSenderLimit("PA2~AR~RM", null)));

        StepVerifier.create(senderLimitUtils.getResidualCapacity(List.of(counterWithNullWeeklyEstimate, counterWithUsedShipmentNull)))
                .assertNext(residualCapacity -> {
                    assertEquals(2, residualCapacity.size());
                    assertEquals(0, residualCapacity.get("2025-01-20~PA1~AR~RM"));
                    assertEquals(5, residualCapacity.get("2025-01-20~PA2~AR~RM"));
                })
                .verifyComplete();
    }

    private PaperDeliveryCounter createCounter(String notificationSentAtWeek, String sk, Integer weeklyEstimate) {
        PaperDeliveryCounter counter = new PaperDeliveryCounter();
        counter.setNotificationSentAtWeek(notificationSentAtWeek);
        counter.setSk(sk);
        counter.setWeeklyEstimate(weeklyEstimate);
        return counter;
    }

    private PaperDeliveryUsedSenderLimit createUsedSenderLimit(String pk, Integer numberOfShipment) {
        PaperDeliveryUsedSenderLimit usedSenderLimit = new PaperDeliveryUsedSenderLimit();
        usedSenderLimit.setPk(pk);
        usedSenderLimit.setNumberOfShipment(numberOfShipment);
        return usedSenderLimit;
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

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.SenderLimitData;
import it.pagopa.pn.delayer.model.SenderLimitJobProcessObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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
        LocalDate deliveryWeek = LocalDate.parse("2026-07-27");
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId =
                Map.of("paid1~productType1~province1", List.of(createPaperDelivery("product1", "cap1", "province1", "senderPaId1", 1)),
                        "paid2~productType2~province2", List.of(createPaperDelivery("product2", "cap2", "province2", "senderPaId2", 1)));
        Map<String, SenderLimitData> senderLimitMaps = new HashMap<>();
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
        assertTrue(senderLimitMaps.containsKey("2026-07-20~key2"));
        assertEquals(62, senderLimitMaps.get("2026-07-20~key2").availableLimit());
        assertEquals(0, senderLimitMaps.get("2026-07-20~key2").incrementUsedLimit());
    }

    @Test
    void retrieveAndEvaluateSenderLimitErrorInPercentage() {
        LocalDate deliveryWeek = LocalDate.now();
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId =
                Map.of("paid1~productType1~province1", List.of(createPaperDelivery("product1", "cap1", "province1", "senderPaId1", 1)),
                        "paid2~productType2~province2", List.of(createPaperDelivery("product2", "cap2", "province2", "senderPaId2", 1)));
        Map<String, SenderLimitData> senderLimitMaps = new HashMap<>();
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
        Map<String, SenderLimitData> senderLimitMaps = Map.of("2026-07-07~paId~AR~RM", new SenderLimitData(100,100,0, 50, LocalDate.now()));

        StepVerifier.create(senderLimitUtils.createIncrementUsedSenderLimitDtos(senderLimitMaps))
                .expectNextMatches(incrementUsedSenderLimitDto -> {
                    assertEquals("paId~AR~RM", incrementUsedSenderLimitDto.pk());
                    assertEquals(50, incrementUsedSenderLimitDto.increment());
                    assertEquals(100, incrementUsedSenderLimitDto.senderLimit());
                    return true;
                })
                .verifyComplete();
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

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SenderLimitUtilsTest {

    private SenderLimitUtils senderLimitUtils;

    @Mock
    private PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @BeforeEach
    void setUp() {
        senderLimitUtils = new SenderLimitUtils(paperDeliverySenderLimitDAO, new PnDelayerUtils(new PnDelayerConfigs()));
    }

    @Test
    void retrieveAndEvaluateSenderLimit() {
        LocalDate deliveryWeek = LocalDate.now();
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId =
                Map.of("key1", List.of(createPaperDelivery("product1", "cap1", "province1", "senderPaId1", 1)),
                        "key2", List.of(createPaperDelivery("product2", "cap2", "province2", "senderPaId2", 1)));
        Map<String, Tuple2<Integer, Integer>> senderLimitMaps = new HashMap<>();
        Integer capacity = 125;
        SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries = new SenderLimitJobPaperDeliveries();

        PaperDeliveryUsedSenderLimit usedSenderLimit = new PaperDeliveryUsedSenderLimit();
        usedSenderLimit.setPk("key1");
        usedSenderLimit.setSenderLimit(50);
        usedSenderLimit.setNumberOfShipment(10);

        PaperDeliverySenderLimit paperDeliverySenderLimit = new PaperDeliverySenderLimit();
        paperDeliverySenderLimit.setPk("key2");
        paperDeliverySenderLimit.setPercentageLimit(50);

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), eq(deliveryWeek)))
                .thenReturn(Flux.just(usedSenderLimit));
        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), eq(deliveryWeek)))
                .thenReturn(Flux.just(paperDeliverySenderLimit));

        senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, senderLimitMaps, capacity, senderLimitJobPaperDeliveries)
                .block();

        assertEquals(2, senderLimitMaps.size());
        assertTrue(senderLimitMaps.containsKey("key1"));
        assertEquals(Tuples.of(50, 10), senderLimitMaps.get("key1"));
        assertTrue(senderLimitMaps.containsKey("key2"));
        assertEquals(Tuples.of(62, 0), senderLimitMaps.get("key2"));
    }

    @Test
    void updateUsedSenderLimit() {
        List<PaperDelivery> paperDeliveryList = List.of(createPaperDelivery("AR", "00100", "RM", "paId", 1),
                createPaperDelivery("AR", "00100", "RM", "paId", 0));
        LocalDate deliveryDate = LocalDate.now();
        Map<String, Tuple2<Integer, Integer>> senderLimitMaps = Map.of("paId~AR~RM", Tuples.of(100, 50));
        when(paperDeliverySenderLimitDAO.updateUsedSenderLimit(anyString(), eq(2L), eq(deliveryDate), anyInt()))
                .thenReturn(Mono.just(2L));

        StepVerifier.create(senderLimitUtils.updateUsedSenderLimit(paperDeliveryList, deliveryDate, senderLimitMaps))
                .expectNext(2L)
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

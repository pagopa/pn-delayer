package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_SENDER_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryUtilsTest {


    private PaperDeliveryUtils paperDeliveryUtils;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        pnDelayerConfigs.setDeliveryDateDayOfWeek(1);
        pnDelayerConfigs.setDeliveryDateInterval(Duration.ofDays(7));
        paperDeliveryUtils = new PaperDeliveryUtils(pnDelayerConfigs);
    }

    @Test
    void filterAndPrepareDeliveries_returnsCorrectSize_whenDeliveriesFitCapacity() {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        paperDelivery1.setPriority(1);
        paperDelivery1.setRequestId("requestId1");
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery2.setPriority(1);
        paperDelivery2.setRequestId("requestId2");
        List<PaperDelivery> deliveries = List.of(paperDelivery1, paperDelivery2);
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(5, 2);
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();

        Integer result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, capacityTuple, deliveriesToSend);

        assertEquals(2, result);
        assertEquals(2, deliveriesToSend.size());
    }

    @Test
    void filterAndPrepareDeliveries_returnsZero_whenNoRemainingCapacity() {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        List<PaperDelivery> deliveries = List.of(paperDelivery1, paperDelivery2);
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(2, 2);
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();

        Integer result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, capacityTuple, deliveriesToSend);

        assertEquals(0, result);
        assertTrue(deliveriesToSend.isEmpty());
    }

    @Test
    void groupDeliveryOnCapAndOrderOnCreatedAt_groupsAndSortsCorrectly() {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        paperDelivery1.setCap("12345");
        paperDelivery1.setCreatedAt(Instant.now().toString());
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery2.setCap("12345");
        paperDelivery2.setCreatedAt(Instant.now().toString());
        PaperDelivery paperDelivery3 = new PaperDelivery();
        paperDelivery3.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery3.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery3.setCap("67890");
        paperDelivery3.setCreatedAt(Instant.now().toString());
        List<PaperDelivery> deliveries = List.of(paperDelivery1, paperDelivery2, paperDelivery3);

        Map<String, List<PaperDelivery>> result = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(deliveries);

        assertEquals(2, result.size());
        assertEquals(List.of(paperDelivery1, paperDelivery2), result.get("12345"));
        assertEquals(List.of(paperDelivery3), result.get("67890"));
    }

    @Test
    void calculateDeliveryWeek_returnsCorrectStartOfWeek() {
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");

        LocalDate result = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);

        assertEquals(LocalDate.parse("2023-10-02"), result);
    }

    @Test
    void calculateSentWeek_returnsCorrectPreviousWeek() {
        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");
        LocalDate result = paperDeliveryUtils.calculateSentWeek(deliveryWeek);
        assertEquals(LocalDate.parse("2023-09-25"), result);
    }
}
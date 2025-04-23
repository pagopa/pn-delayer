package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.model.PaperDeliveryTransactionRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.util.function.Tuples;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryUtilsTest {

    @Mock
    private PnDelayerConfigs pnDelayerConfig;

    @InjectMocks
    private PaperDeliveryUtils paperDeliveryUtils;

    @Test
    void filterAndPrepareDeliveriesNoCapacity() {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        int result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, Tuples.of(100, 100));

        assertEquals(0, result);
        assertEquals(0, transactionRequest.getPaperDeliveryHighPriorityList().size());
        assertEquals(0, transactionRequest.getPaperDeliveryReadyToSendList().size());
    }

    @Test
    void filterAndPrepareDeliveriesLessCapacityThanDeliveries() {
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1);
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(1));
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));

        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        int result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, Tuples.of(100, 95));

        assertEquals(5, result);
        assertEquals(5, transactionRequest.getPaperDeliveryHighPriorityList().size());
        assertEquals(5, transactionRequest.getPaperDeliveryReadyToSendList().size());
    }

    @Test
    void filterAndPrepareDeliveriesAllCapacity() {
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1);
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(1));
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));

        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        int result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, Tuples.of(100, 0));

        assertEquals(10, result);
        assertEquals(10, transactionRequest.getPaperDeliveryHighPriorityList().size());
        assertEquals(10, transactionRequest.getPaperDeliveryReadyToSendList().size());
    }

    @Test
    void filterAndPrepareDeliveriesAllCapacityVerifyPartitionOnDay() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(1));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1); //Lunedì
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        int result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, Tuples.of(12, 0));

        assertEquals(10, result);
        assertEquals(10, transactionRequest.getPaperDeliveryHighPriorityList().size());
        assertEquals(10, transactionRequest.getPaperDeliveryReadyToSendList().size());

        assertEquals(2, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
        assertEquals(2, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(1, ChronoUnit.DAYS))).count());
        assertEquals(2, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(2, ChronoUnit.DAYS))).count());
        assertEquals(2, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(3, ChronoUnit.DAYS))).count());
        assertEquals(2, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(4, ChronoUnit.DAYS))).count());
    }

    @Test
    void filterAndPrepareDeliveriesAllCapacityVerifyPartitionOnHours() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofHours(12));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1);
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, Tuples.of(10, 2));

        assertEquals(0, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
        assertEquals(0, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(12, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(24, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(36, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(48, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(60, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(72, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(84, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(96, ChronoUnit.HOURS))).count());
        assertEquals(1, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(108, ChronoUnit.HOURS))).count());


    }

    @Test
    void enrichWithDeliveryDateOnWeek() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(7));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1); //Lunedì
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, transactionRequest, Tuples.of(12, 2));

        assertEquals(10, transactionRequest.getPaperDeliveryReadyToSendList().stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
    }


    @Test
    void checkCapacityAndFilterList_returnsFilteredList() {
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();
        List<PaperDeliveryHighPriority> result = paperDeliveryUtils.checkCapacityAndFilterList(Tuples.of(10,5), deliveries);

        assertEquals(5, result.size());
    }

    @Test
    void checkCapacityAndFilterList_returnsGivenLists() {
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();
        List<PaperDeliveryHighPriority> result = paperDeliveryUtils.checkCapacityAndFilterList(Tuples.of(10,0), deliveries);

        assertEquals(10, result.size());
    }

    @Test
    void checkListsSizeOk() {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        transactionRequest.setPaperDeliveryHighPriorityList(getHighPriorityDeliveries());
        transactionRequest.setPaperDeliveryReadyToSendList(getReadyToSendDeliveries());
        assertTrue(paperDeliveryUtils.checkListsSize(transactionRequest));
    }

    @Test
    void checkListsSizeEmptyHighPriorities() {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        transactionRequest.setPaperDeliveryReadyToSendList(getReadyToSendDeliveries());

        assertFalse(paperDeliveryUtils.checkListsSize(transactionRequest));
    }

    @Test
    void checkListsSizeEmptyReadyToSend() {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        transactionRequest.setPaperDeliveryHighPriorityList(getHighPriorityDeliveries());

        assertFalse(paperDeliveryUtils.checkListsSize(transactionRequest));
    }

    @Test
    void checkListsSizeAllListsEmpty() {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        assertFalse(paperDeliveryUtils.checkListsSize(transactionRequest));
    }

    @Test
    void checkListsSizeListsHaveDifferentSize() {
        PaperDeliveryTransactionRequest transactionRequest = new PaperDeliveryTransactionRequest();
        List<PaperDeliveryHighPriority> highPriorityDeliveries = getHighPriorityDeliveries();
        highPriorityDeliveries.remove(0);
        transactionRequest.setPaperDeliveryHighPriorityList(highPriorityDeliveries);
        transactionRequest.setPaperDeliveryReadyToSendList(getReadyToSendDeliveries());

        assertFalse(paperDeliveryUtils.checkListsSize(transactionRequest));
    }


    @Test
    void groupDeliveryOnCapAndOrderOnCreatedAt_groupsAndSortsCorrectly() {
        List<PaperDeliveryHighPriority> deliveries = getHighPriorityDeliveries();

        Map<String, List<PaperDeliveryHighPriority>> result = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(deliveries);

        assertEquals(2, result.keySet().size());
        assertEquals(5, result.get("00100").size());
        assertEquals(5, result.get("00200").size());
    }

    @Test
    void calculateDeliveryWeekWithCutOff1() {
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1);
        Instant createdAt = Instant.parse("2025-04-01T10:00:00Z");
        Instant result = paperDeliveryUtils.calculateDeliveryWeek(createdAt);
        assertEquals(Instant.parse("2025-04-07T00:00:00Z"), result);
    }

    @Test
    void calculateDeliveryWeekWithCutOff2() {
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(7));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(3);
        Instant createdAt = Instant.parse("2025-04-07T00:00:00Z");
        Instant result = paperDeliveryUtils.calculateDeliveryWeek(createdAt);
        assertEquals(Instant.parse("2025-04-09T00:00:00Z"), result);
    }

    @Test
    void calculateDeliveryWeekNoCutOff1() {
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(0));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1);
        Instant createdAt = Instant.parse("2025-04-01T10:00:00Z");
        Instant result = paperDeliveryUtils.calculateDeliveryWeek(createdAt);
        assertEquals(Instant.parse("2025-03-31T00:00:00Z"), result);
    }

    @Test
    void calculateDeliveryWeekNoCutOff2() {
        when(pnDelayerConfig.getPaperDeliveryCutOffDuration()).thenReturn(Duration.ofDays(0));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(3);
        Instant createdAt = Instant.parse("2025-04-07T00:00:00Z");
        Instant result = paperDeliveryUtils.calculateDeliveryWeek(createdAt);
        assertEquals(Instant.parse("2025-04-02T00:00:00Z"), result);
    }

    private static List<PaperDeliveryHighPriority> getHighPriorityDeliveries() {
        List<PaperDeliveryHighPriority> deliveries = new ArrayList<>();
        IntStream.range(0,10).forEach(i -> {
            PaperDeliveryHighPriority item = new PaperDeliveryHighPriority();
            item.setRequestId("item" + i);
            item.setCap(i % 2 == 0 ? "00100" : "00200");
            item.setCreatedAt(Instant.now().plus(Duration.ofSeconds(i)));
            deliveries.add(item);
        });
        return deliveries;
    }

    private static List<PaperDeliveryReadyToSend> getReadyToSendDeliveries() {
        List<PaperDeliveryReadyToSend> deliveries = new ArrayList<>();
        IntStream.range(0,10).forEach(i -> {
            PaperDeliveryReadyToSend item = new PaperDeliveryReadyToSend();
            item.setRequestId("item" + i);
            deliveries.add(item);
        });
        return deliveries;
    }
}
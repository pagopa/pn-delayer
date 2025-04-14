package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryUtilsTest {

    @Mock
    private PnDelayerConfigs pnDelayerConfig;

    @InjectMocks
    private PaperDeliveryUtils paperDeliveryUtils;


    @Test
    void enrichWithDeliveryDateOnDay() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(1));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1); //Lunedì

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        List<PaperDeliveryReadyToSend> tempItems = new ArrayList<>();
        IntStream.range(0, 50)
                .forEach(i -> {
                    PaperDeliveryReadyToSend item = new PaperDeliveryReadyToSend();
                    item.setRequestId("item" + i);
                    tempItems.add(item);
                });

        paperDeliveryUtils.enrichWithDeliveryDate(tempItems, 140, 10);

        assertEquals(10, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
        assertEquals(20, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(1, ChronoUnit.DAYS))).count());
        assertEquals(20, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(2, ChronoUnit.DAYS))).count());
    }

    @Test
    void enrichWithDeliveryDateOnDayExcess() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(1));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1); //Lunedì

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        List<PaperDeliveryReadyToSend> tempItems = new ArrayList<>();
        IntStream.range(0, 50)
                .forEach(i -> {
                    PaperDeliveryReadyToSend item = new PaperDeliveryReadyToSend();
                    item.setRequestId("item" + i);
                    tempItems.add(item);
                });

        paperDeliveryUtils.enrichWithDeliveryDate(tempItems, 50, 10);

        assertEquals(0, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
        assertEquals(6, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(1, ChronoUnit.DAYS))).count());
        assertEquals(8, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(2, ChronoUnit.DAYS))).count());
        assertEquals(8, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(3, ChronoUnit.DAYS))).count());
        assertEquals(8, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(4, ChronoUnit.DAYS))).count());
        assertEquals(8, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(5, ChronoUnit.DAYS))).count());
        assertEquals(12, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(6, ChronoUnit.DAYS))).count());
    }


    @Test
    void enrichWithDeliveryDateOnHours() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofHours(12));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1); //Lunedì

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        List<PaperDeliveryReadyToSend> tempItems = new ArrayList<>();
        IntStream.range(0, 50)
                .forEach(i -> {
                    PaperDeliveryReadyToSend item = new PaperDeliveryReadyToSend();
                    item.setRequestId("item" + i);
                    tempItems.add(item);
                });

        paperDeliveryUtils.enrichWithDeliveryDate(tempItems, 140, 10);

        assertEquals(0, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
        assertEquals(10, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(12, ChronoUnit.HOURS))).count());
        assertEquals(10, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(24, ChronoUnit.HOURS))).count());
        assertEquals(10, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(36, ChronoUnit.HOURS))).count());
        assertEquals(10, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(48, ChronoUnit.HOURS))).count());
        assertEquals(10, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart.plus(60, ChronoUnit.HOURS))).count());
    }

    @Test
    void enrichWithDeliveryDateOnWeek() {
        when(pnDelayerConfig.getDeliveryDateInterval()).thenReturn(Duration.ofDays(7));
        when(pnDelayerConfig.getDeliveryDateDayOfWeek()).thenReturn(1); //Lunedì

        Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1)))
                .atStartOfDay().toInstant(ZoneOffset.UTC);

        List<PaperDeliveryReadyToSend> tempItems = new ArrayList<>();
        IntStream.range(0, 50)
                .forEach(i -> {
                    PaperDeliveryReadyToSend item = new PaperDeliveryReadyToSend();
                    item.setRequestId("item" + i);
                    tempItems.add(item);
                });

        paperDeliveryUtils.enrichWithDeliveryDate(tempItems, 140, 10);

        assertEquals(50, tempItems.stream().filter(item -> item.getDeliveryDate().equals(weekDayStart)).count());
    }
}
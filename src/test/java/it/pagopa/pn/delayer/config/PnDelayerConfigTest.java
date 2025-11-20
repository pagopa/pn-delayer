package it.pagopa.pn.delayer.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class PnDelayerConfigTest {

    @Test
    void calculateDailyExecutionNumberActive() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelFirstSchedulerCron("0 1-20 ? * TUE-SUN *");
        configs.setDelayerToPaperChannelSecondSchedulerCron("0 1-10 ? * TUE-SUN *");
        configs.setDelayerToPaperChannelFirstSchedulerStartDate(Instant.now().minus(1, ChronoUnit.DAYS));
        configs.setDelayerToPaperChannelSecondSchedulerStartDate(Instant.now().plus(1, ChronoUnit.DAYS));

        int count = configs.calculateDailyExecutionNumber(LocalDate.now());
        Assertions.assertEquals(20, count);
    }

    @Test
    void calculateDailyExecutionNumberNext() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelFirstSchedulerCron("0 1-20 ? * TUE-SUN *");
        configs.setDelayerToPaperChannelSecondSchedulerCron("0 1-10 ? * TUE-SUN *");
        configs.setDelayerToPaperChannelFirstSchedulerStartDate(Instant.now().plus(1, ChronoUnit.DAYS));
        configs.setDelayerToPaperChannelSecondSchedulerStartDate(Instant.now().minus(1, ChronoUnit.DAYS));

        int count = configs.calculateDailyExecutionNumber(LocalDate.now());
        Assertions.assertEquals(10, count);
    }

    @Test
    void calculateDailyExecutionNumberBothDateAfter() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelFirstSchedulerCron(null);
        configs.setDelayerToPaperChannelSecondSchedulerCron(null);
        configs.setDelayerToPaperChannelFirstSchedulerStartDate(Instant.now().plus(1, ChronoUnit.DAYS));
        configs.setDelayerToPaperChannelSecondSchedulerStartDate(Instant.now().plus(1, ChronoUnit.DAYS));

        Assertions.assertThrows(RuntimeException.class, () -> configs.calculateDailyExecutionNumber(LocalDate.now()));
    }

    @Test
    void calculateDailyExecutionNumberBothDateBefore() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelFirstSchedulerCron("0 1-20 ? * TUE-SUN *");
        configs.setDelayerToPaperChannelSecondSchedulerCron("0 1-10 ? * TUE-SUN *");
        configs.setDelayerToPaperChannelFirstSchedulerStartDate(Instant.now().minus(1, ChronoUnit.DAYS));
        configs.setDelayerToPaperChannelSecondSchedulerStartDate(Instant.now().minus(2, ChronoUnit.DAYS));

        int count = configs.calculateDailyExecutionNumber(LocalDate.now());
        Assertions.assertEquals(20, count);
    }
}

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CronUtilsTest {

    @Test
    void testCronCustom() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelFirstSchedulerCron("0 1-20 ? * TUE-SUN *");

        int count = CronUtils.countExecutionsInNextScheduledDay(configs.getDelayerToPaperChannelFirstSchedulerCron());
        Assertions.assertEquals(20, count);

    }
}
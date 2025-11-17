package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CronUtilsTest {

    @Test
    void testWithoutMondayCronCustom() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelDailyScheduleCron("0 4-20 ? * MON-SUN *");
        configs.setMondayDelayerToPaperChannelDailyScheduleCron(null);

        int count = CronUtils.countExecutionsInNextScheduledDay(configs.getDelayerToPaperChannelDailyScheduleCron());
        Assertions.assertEquals(17, count);

    }

    @Test
    void testWithMondayCronCustom() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelDailyScheduleCron("0 1-20 ? * TUE-SUN *");
        configs.setMondayDelayerToPaperChannelDailyScheduleCron("0 4-20 ? * MON *");

        int count = CronUtils.countExecutionsInNextScheduledDay(configs.getDelayerToPaperChannelDailyScheduleCron());
        int mondayCount = CronUtils.countExecutionsInNextScheduledDay(configs.getMondayDelayerToPaperChannelDailyScheduleCron());
        Assertions.assertEquals(20, count);
        Assertions.assertEquals(17, mondayCount);

    }
}

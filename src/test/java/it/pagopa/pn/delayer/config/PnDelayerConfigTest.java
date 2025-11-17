package it.pagopa.pn.delayer.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PnDelayerConfigTest {

    @Test
    void testWithoutMondayCronCustom() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelDailyScheduleCron("0 4-20 ? * MON-SUN *");
        configs.setMondayDelayerToPaperChannelDailyScheduleCron(null);

        int mondayCount = configs.calculateMondayExecutionNumber();
        int count = configs.calculateDailyExecutionNumber();
        Assertions.assertEquals(17, count);
        Assertions.assertEquals(17, mondayCount);
    }

    @Test
    void testWithMondayCronCustom() {

        PnDelayerConfigs configs = new PnDelayerConfigs();
        configs.setDelayerToPaperChannelDailyScheduleCron("0 1-20 ? * TUE-SUN *");
        configs.setMondayDelayerToPaperChannelDailyScheduleCron("0 4-20 ? * MON *");

        int mondayCount = configs.calculateMondayExecutionNumber();
        int count = configs.calculateDailyExecutionNumber();
        Assertions.assertEquals(20, count);
        Assertions.assertEquals(17, mondayCount);

    }
}

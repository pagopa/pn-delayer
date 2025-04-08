package it.pagopa.pn.delayer.config;

import it.pagopa.pn.commons.conf.SharedAutoConfiguration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.time.Duration;

@Configuration
@ConfigurationProperties(prefix = "pn.delayer")
@Data
@Import({SharedAutoConfiguration.class})
public class PnDelayerConfigs {

    private Dao dao;
    private int deliveryDateDayOfWeek; //1-7 the day-of-week to represent, from 1 (Monday) to 7 (Sunday)
    private int highPriorityQuerySize;
    private Duration deliveryDateInterval;

    @Data
    public static class Dao {
        private String paperDeliveryDriverCapacitiesTableName;
        private String paperDeliveryDriverCapacitiesDispatchedTableName;
        private String paperDeliveryHighPriorityTableName;
        private String paperDeliveryReadyToSendTableName;
    }
}

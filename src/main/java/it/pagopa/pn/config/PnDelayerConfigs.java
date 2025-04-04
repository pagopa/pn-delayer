package it.pagopa.pn.config;

import it.pagopa.pn.commons.conf.SharedAutoConfiguration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConfigurationProperties(prefix = "pn.delayer")
@Data
@Import({SharedAutoConfiguration.class})
public class PnDelayerConfigs {

    private Dao dao;

    @Data
    public static class Dao {
        private String paperDeliveryDriverCapacitiesTableName;
        private String paperDeliveryDriverCapacitiesDispatchedTableName;
        private String paperDeliveryHighPriorityTableName;
        private String paperDeliveryReadyToSendTableName;
    }
}

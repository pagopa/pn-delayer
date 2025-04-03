package it.pagopa.pn.delayer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import it.pagopa.pn.commons.conf.SharedAutoConfiguration;


@Configuration
@ConfigurationProperties( prefix = "pn.delayer" )
@Data
@Import({SharedAutoConfiguration.class})
public class PnDelayerConfig {
    private int deliveryDateDayOfWeek; //1-7 the day-of-week to represent, from 1 (Monday) to 7 (Sunday)
    private int highPriorityQuerySize;
}

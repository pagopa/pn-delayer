package it.pagopa.pn.delayer.config;

import it.pagopa.pn.commons.conf.SharedAutoConfiguration;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "pn.delayer")
@Data
@Import({SharedAutoConfiguration.class})
public class PnDelayerConfigs {

    public static final String IMPLEMENTATION_TYPE_PROPERTY_NAME = "pn.delayer.storage.impl";

    private Dao dao;
    private EvaluateDriverCapacityJobInput evaluateDriverCapacityJobInput;
    private EvaluateSenderLimitJobInput evaluateSenderLimitJobInput;
    private WorkflowStepEnum workflowStep;
    private int deliveryDateDayOfWeek; //1-7 the day-of-week to represent, from 1 (Monday) to 7 (Sunday)
    private Duration deliveryDateInterval;
    private String actualTenderId;


    @Data
    public static class EvaluateDriverCapacityJobInput {
        private String unifiedDeliveryDriver;
        private List<String> provinceList;
    }

    @Data
    public static class EvaluateSenderLimitJobInput {
        private String province;
    }


    @Data
    public static class Dao {
        private String paperDeliveryDriverCapacitiesTableName;
        private String paperDeliveryDriverUsedCapacitiesTableName;
        private String paperDeliveryTableName;
        private Integer paperDeliveryQueryLimit;
        private String paperDeliverySenderLimitTableName;
        private String paperDeliveryUsedSenderLimitTableName;
    }
}

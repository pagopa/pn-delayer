package it.pagopa.pn.delayer.config;

import it.pagopa.pn.commons.conf.SharedAutoConfiguration;
import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.CronUtils;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
    private EvaluateResidualCapacityJobInput evaluateResidualCapacityJobInput;
    private WorkflowStepEnum workflowStep;
    private Integer deliveryDateDayOfWeek; //1-7 the day-of-week to represent, from 1 (Monday) to 7 (Sunday)
    private List<String> printCapacity;
    private Duration printCounterTtlDuration;
    private String actualTenderId;
    private Integer printCapacityWeeklyWorkingDays;
    private String paperDeliveryPriorityParameterName;
    private String PaperChannelTenderApiLambdaArn;
    private LocalDate deliveryWeek;
    private String delayerToPaperChannelFirstSchedulerCron;
    private String delayerToPaperChannelSecondSchedulerCron;
    private Instant delayerToPaperChannelFirstSchedulerStartDate;
    private Instant delayerToPaperChannelSecondSchedulerStartDate;

    @Data
    public static class EvaluateDriverCapacityJobInput {
        private String unifiedDeliveryDriver;
        private String provinceList;
    }

    @Data
    public static class EvaluateResidualCapacityJobInput {
        private String unifiedDeliveryDriver;
        private String provinceList;
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
        private String paperDeliveryCounterTableName;
        private String paperDeliveryPrintCapacityTableName;
    }

    public Integer calculateDailyExecutionNumber() {
        Instant now = Instant.now();

        boolean activeSchedulerBeforeDeliveryDate = delayerToPaperChannelFirstSchedulerStartDate.isBefore(now);
        boolean nextSchedulerBeforeDeliveryDate   = delayerToPaperChannelSecondSchedulerStartDate.isBefore(now);

        if (!activeSchedulerBeforeDeliveryDate && !nextSchedulerBeforeDeliveryDate) {
            throw new PnInternalException("Both scheduler start dates are after the delivery date", "ERROR_SCHEDULERS_START_AFTER_DELIVERY_DATE");
        }

        String cron = retrieveCron(activeSchedulerBeforeDeliveryDate, nextSchedulerBeforeDeliveryDate);

        return CronUtils.countExecutionsInNextScheduledDay(cron);
    }

    private String retrieveCron(boolean activeSchedulerBeforeDeliveryDate, boolean nextSchedulerBeforeDeliveryDate) {
        String cron;
        if (activeSchedulerBeforeDeliveryDate && nextSchedulerBeforeDeliveryDate) {
            cron = delayerToPaperChannelFirstSchedulerStartDate.isAfter(delayerToPaperChannelSecondSchedulerStartDate)
                    ? delayerToPaperChannelFirstSchedulerCron
                    : delayerToPaperChannelSecondSchedulerCron;
        } else {
            cron = activeSchedulerBeforeDeliveryDate ? delayerToPaperChannelFirstSchedulerCron : delayerToPaperChannelSecondSchedulerCron;
        }
        return cron;
    }
}

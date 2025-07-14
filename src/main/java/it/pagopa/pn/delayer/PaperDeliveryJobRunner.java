package it.pagopa.pn.delayer;

import it.pagopa.pn.commons.utils.MDCUtils;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.service.DriverCapacityJobService;
import it.pagopa.pn.delayer.service.SenderLimitJobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
@Profile("!test")
public class PaperDeliveryJobRunner implements CommandLineRunner {

    private final DriverCapacityJobService driverCapacityJobService;
    private final SenderLimitJobService senderLimitJobService;
    private final ApplicationContext applicationContext;
    private final PnDelayerConfigs pnDelayerConfigs;

    @Override
    public void run(String... args) {
        int exitCode;
        WorkflowStepEnum workflowStep = pnDelayerConfigs.getWorkflowStep();
        switch (workflowStep) {
            case EVALUATE_SENDER_LIMIT:
                log.info("Starting Evaluate Sender Limit step");
                exitCode = executeEvaluateSendLimitStep();
                break;
            case EVALUATE_DRIVER_CAPACITY:
                log.info("Starting Evaluate Driver Capacity step");
                exitCode = executeEvaluateDriverCapacityStep();
                break;
            default:
                log.error("Unknown workflow step: {}", workflowStep);
                exitCode = SpringApplication.exit(applicationContext, () -> 1);
                System.exit(exitCode);
                return;
        }
        log.info("Batch finished with exit code: {}", exitCode);
        System.exit(exitCode);
    }

    private int executeEvaluateDriverCapacityStep() {
        String unifiedDeliveryDriver = pnDelayerConfigs.getEvaluateDriverCapacityJobInput().getUnifiedDeliveryDriver();
        String jobIndex = System.getenv("AWS_BATCH_JOB_ARRAY_INDEX");
        if (StringUtils.hasText(jobIndex)) {
            return Optional.of(jobIndex)
                    .map(Integer::parseInt)
                    .map(index -> pnDelayerConfigs.getEvaluateDriverCapacityJobInput().getProvinceList().get(index))
                    .map(province -> {
                        log.info("Starting batch for unifiedDeliveryDriver: {} and province: {}", unifiedDeliveryDriver, province);
                        addMDC( String.join("~", unifiedDeliveryDriver,  province));
                        try {
                            var startExecutionBatch = Instant.now();
                            Mono<Void> monoExcecution = driverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, new HashMap<>(), startExecutionBatch, pnDelayerConfigs.getActualTenderId());
                            MDCUtils.addMDCToContextAndExecute(monoExcecution).block();return 0;
                        } catch (Exception e) {
                            log.error("Error while executing batch", e);
                            return 1;
                        }
                    }).orElseGet(() -> {
                        log.error("Province on index [{}] not found, cannot start batch", jobIndex);
                        return SpringApplication.exit(applicationContext, () -> 1);
                    });
        } else {
            log.error("No job index found, cannot start batch");
            return SpringApplication.exit(applicationContext, () -> 1);
        }
    }

    private int executeEvaluateSendLimitStep() {
        String province = pnDelayerConfigs.getEvaluateSenderLimitJobInput().getProvince();
        log.info("Starting batch for province: {}", province);
        addMDC(province);
        try {
            var startExecutionBatch = Instant.now();
            Mono<Void> monoExcecution = senderLimitJobService.startSenderLimitJob(province, new HashMap<>(), startExecutionBatch);
            MDCUtils.addMDCToContextAndExecute(monoExcecution).block();
            return 0;
        } catch (Exception e) {
            log.error("Error while executing batch", e);
            return 1;
        }
    }


    private void addMDC(String requestId) {
        MDCUtils.clearMDCKeys();
        MDC.put(MDCUtils.MDC_TRACE_ID_KEY, StringUtils.hasText(System.getenv("AWS_BATCH_JOB_ID"))
                ? System.getenv("AWS_BATCH_JOB_ID")
                : UUID.randomUUID().toString());
        MDC.put(MDCUtils.MDC_PN_CTX_REQUEST_ID, requestId);
    }
}


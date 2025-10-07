package it.pagopa.pn.delayer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.commons.utils.MDCUtils;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.service.EvaluateDriverCapacityJobService;
import it.pagopa.pn.delayer.service.EvaluateResidualCapacityJobService;
import it.pagopa.pn.delayer.service.EvaluateSenderLimitJobService;
import it.pagopa.pn.delayer.utils.PnDelayerUtils;
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
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
@Profile("!test")
public class PaperDeliveryJobRunner implements CommandLineRunner {

    private final EvaluateDriverCapacityJobService evaluateDriverCapacityJobService;
    private final EvaluateSenderLimitJobService evaluateSenderLimitJobService;
    private final EvaluateResidualCapacityJobService evaluateResidualCapacityJobService;
    private final ApplicationContext applicationContext;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final PnDelayerUtils pnDelayerUtils;
    private final ObjectMapper objectMapper;

    @Override
    public void run(String... args) throws JsonProcessingException {
        int exitCode;
        WorkflowStepEnum workflowStep = pnDelayerConfigs.getWorkflowStep();
        switch (workflowStep) {
            case EVALUATE_SENDER_LIMIT:
                log.info("Starting Evaluate Sender Limit step");
                exitCode = executeEvaluateSenderLimitStep();
                break;
            case EVALUATE_DRIVER_CAPACITY:
                log.info("Starting Evaluate Driver Capacity step");
                exitCode = executeEvaluateDriverCapacityStep();
                break;
            case EVALUATE_RESIDUAL_CAPACITY:
                log.info("Starting Evaluate Residual Capacity step");
                exitCode = executeEvaluateResidualCapacityStep();
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

    private int executeEvaluateResidualCapacityStep() throws JsonProcessingException {
        String unifiedDeliveryDriver = pnDelayerConfigs.getEvaluateResidualCapacityJobInput().getUnifiedDeliveryDriver();
        String provinces = pnDelayerConfigs.getEvaluateResidualCapacityJobInput().getProvinceList();
        LocalDate deliveryWeek = Objects.isNull(pnDelayerConfigs.getDeliveryWeek()) ? pnDelayerUtils.calculateDeliveryWeek(Instant.now()) : pnDelayerConfigs.getDeliveryWeek();
        List<String> provinceList = objectMapper.readValue(provinces, new TypeReference<>() {});
        String jobIndex = System.getenv("AWS_BATCH_JOB_ARRAY_INDEX");
        if (StringUtils.hasText(jobIndex)) {
            return Optional.of(jobIndex)
                    .map(Integer::parseInt)
                    .map(provinceList::get)
                    .map(province -> {
                        log.info("Starting batch for unifiedDeliveryDriver: {} and province: {}", unifiedDeliveryDriver, province);
                        addMDC( String.join("~", unifiedDeliveryDriver,  province));
                        try {
                            Mono<Void> monoExcecution = evaluateResidualCapacityJobService.startEvaluateResidualCapacityJob(unifiedDeliveryDriver, province, deliveryWeek, pnDelayerConfigs.getActualTenderId());
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

    private int executeEvaluateDriverCapacityStep() throws JsonProcessingException {
        String unifiedDeliveryDriver = pnDelayerConfigs.getEvaluateDriverCapacityJobInput().getUnifiedDeliveryDriver();
        String provinces = pnDelayerConfigs.getEvaluateDriverCapacityJobInput().getProvinceList();
        LocalDate deliveryWeek = Objects.isNull(pnDelayerConfigs.getDeliveryWeek()) ? pnDelayerUtils.calculateDeliveryWeek(Instant.now()) : pnDelayerConfigs.getDeliveryWeek();
        List<String> provinceList = objectMapper.readValue(provinces, new TypeReference<>() {});
        String jobIndex = System.getenv("AWS_BATCH_JOB_ARRAY_INDEX");
        if (StringUtils.hasText(jobIndex)) {
            return Optional.of(jobIndex)
                    .map(Integer::parseInt)
                    .map(provinceList::get)
                    .map(province -> {
                        log.info("Starting batch for unifiedDeliveryDriver: {} and province: {}", unifiedDeliveryDriver, province);
                        addMDC( String.join("~", unifiedDeliveryDriver,  province));
                        try {
                            Mono<Void> monoExcecution = evaluateDriverCapacityJobService.startEvaluateDriverCapacityJob(unifiedDeliveryDriver, province, deliveryWeek, pnDelayerConfigs.getActualTenderId());
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

    private int executeEvaluateSenderLimitStep() {
        String province = pnDelayerConfigs.getEvaluateSenderLimitJobInput().getProvince();
        String tenderId = pnDelayerConfigs.getActualTenderId();
        LocalDate deliveryWeek = Objects.isNull(pnDelayerConfigs.getDeliveryWeek()) ? pnDelayerUtils.calculateDeliveryWeek(Instant.now()) : pnDelayerConfigs.getDeliveryWeek();
        log.info("Starting batch for province: {}", province);
        addMDC(province);
        try {
            Mono<Void> monoExcecution = evaluateSenderLimitJobService.startSenderLimitJob(province, tenderId, deliveryWeek);
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


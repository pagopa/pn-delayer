package it.pagopa.pn.delayer;

import it.pagopa.pn.commons.utils.MDCUtils;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.service.HighPriorityBatchService;
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

    private final HighPriorityBatchService highPriorityBatchService;
    private final ApplicationContext applicationContext;
    private final PnDelayerConfigs pnDelayerConfigs;

    @Override
    public void run(String... args) {
        int exitCode;
        String unifiedDeliveryDriver = pnDelayerConfigs.getJobInput().getUnifiedDeliveryDriver();
        String jobIndex = System.getenv("AWS_BATCH_JOB_ARRAY_INDEX");
        if (StringUtils.hasText(jobIndex)) {
            exitCode = Optional.of(jobIndex)
                    .map(Integer::parseInt)
                    .map(index -> pnDelayerConfigs.getJobInput().getProvinceList().get(index))
                    .map(province -> {
                        String unifiedDeliveryDriverProvince = String.join("~", unifiedDeliveryDriver, province);
                        log.info("Starting batch for pk: {}", unifiedDeliveryDriverProvince);
                        addMDC(unifiedDeliveryDriverProvince);
                        return doExecute(unifiedDeliveryDriverProvince);
                    }).orElseGet(() -> {
                        log.error("Province on index [{}] not found, cannot start batch", jobIndex);
                        return SpringApplication.exit(applicationContext, () -> 1);
                    });
        } else {
            log.error("No job index found, cannot start batch");
            exitCode = SpringApplication.exit(applicationContext, () -> 1);
        }
        log.info("Batch finished with exit code: {}", exitCode);
        System.exit(exitCode);
    }

    private int doExecute(String unifiedDeliveryDriverProvince) {
        try {
            var startExecutionBatch = Instant.now();
            Mono<Void> monoExcecution = highPriorityBatchService.initHighPriorityBatch(unifiedDeliveryDriverProvince, new HashMap<>(), startExecutionBatch);
            MDCUtils.addMDCToContextAndExecute(monoExcecution).block();
            return 0;
        } catch (Exception e) {
            log.error("Error while executing batch", e);
            return 1;
        }
    }

    private void addMDC(String unifiedDeliveryDriverProvince) {
        MDCUtils.clearMDCKeys();
        MDC.put(MDCUtils.MDC_TRACE_ID_KEY, StringUtils.hasText(System.getenv("AWS_BATCH_JOB_ID"))
                ? System.getenv("AWS_BATCH_JOB_ID") : UUID.randomUUID().toString());
        MDC.put(MDCUtils.MDC_PN_CTX_REQUEST_ID, unifiedDeliveryDriverProvince);
    }
}


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
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaperDeliveryJobRunner implements CommandLineRunner {

    private final HighPriorityBatchService highPriorityBatchService;
    private final ApplicationContext applicationContext;
    private final PnDelayerConfigs pnDelayerConfigs;

    @Override
    public void run(String... args) {
        int exitCode;
        String unifiedDeliveryDriverProvince = pnDelayerConfigs.getUnifiedDeliveryDriverProvince();
        addMDC(unifiedDeliveryDriverProvince);
        if (StringUtils.hasText(unifiedDeliveryDriverProvince)) {
            exitCode = doExecute(unifiedDeliveryDriverProvince);
        } else {
            log.warn("No pk provided, cannot start batch");
            exitCode = SpringApplication.exit(applicationContext, () -> 1);
        }
        System.exit(exitCode);
    }

    private int doExecute(String unifiedDeliveryDriverProvince) {
        try {
            log.info("Starting batch for pk: {}", unifiedDeliveryDriverProvince);
            var startExecutionBatch = Instant.now();
            Mono<Void> monoExcecution = highPriorityBatchService.initHighPriorityBatch(unifiedDeliveryDriverProvince, new HashMap<>(), startExecutionBatch);
            MDCUtils.addMDCToContextAndExecute(monoExcecution).block();
            return 0;
        }
        catch (Exception e) {
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


package it.pagopa.pn.delayer;

import it.pagopa.pn.delayer.service.HighPriorityBatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaperDeliveryJobRunner implements CommandLineRunner {

    private final HighPriorityBatchService highPriorityBatchService;
    private final ApplicationContext applicationContext;

    @Override
    public void run(String... args) {
        if(args.length != 0 && StringUtils.isNotBlank(args[0])){
            log.info("Starting batch for pk: {}", args[0]);
            var startExecutionBatch = Instant.now();
            highPriorityBatchService.initHighPriorityBatch(args[0], new HashMap<>(), startExecutionBatch).block();
            int exitCode = SpringApplication.exit(applicationContext, () -> 0);
            System.exit(exitCode);
        } else {
            log.warn("No pk provided, cannot start batch");
        }
    }
}


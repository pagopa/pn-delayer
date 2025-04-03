package it.pagopa.pn.delayer;

import it.pagopa.pn.delayer.service.HighPriorityBatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaperDeliveryJobRunner implements CommandLineRunner {

    private final HighPriorityBatchService highPriorityBatchService;
    private final ApplicationContext applicationContext;

    @Override
    public void run(String... args) {
        String inputParameter = args[0];
        if (StringUtils.isNotBlank(inputParameter)) {
            log.info("Starting batch for pk: {}", inputParameter);
            highPriorityBatchService.initHighPriorityBatch(inputParameter).block();
            int exitCode = SpringApplication.exit(applicationContext, () -> 0);
            System.exit(exitCode);
        } else {
            log.warn("No pk provided, cannot start batch");
        }
    }
}

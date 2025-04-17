package it.pagopa.pn.delayer.rest;

import it.pagopa.pn.delayer.service.HighPriorityBatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;

@RestController
@RequiredArgsConstructor
@Slf4j
public class TestController {

    private final HighPriorityBatchService highPriorityBatchService;

    @GetMapping("/delayer-private/execute-job")
    public Mono<Void> executeJob(@RequestParam(value = "deliveryDriverIdGeoKey") String pk) {
        return highPriorityBatchService.initHighPriorityBatch(pk, new HashMap<>(), Instant.now())
                .doOnSuccess(result -> log.info("Batch completed"));
    }
}

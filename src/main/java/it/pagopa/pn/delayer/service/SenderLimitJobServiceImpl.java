package it.pagopa.pn.delayer.service;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.Map;

@Component
public class SenderLimitJobServiceImpl implements SenderLimitJobService {

    @Override
    public Mono<Void> startSenderLimitJob(String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecution) {
        return Mono.empty();
    }
}

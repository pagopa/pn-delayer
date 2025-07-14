package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.Map;

public interface SenderLimitJobService {

    Mono<Void> startSenderLimitJob(String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecution);
}

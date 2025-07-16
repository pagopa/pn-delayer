package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.Map;

public interface EvaluateSenderLimitJobService {

    Mono<Void> startSenderLimitJob(String province, String tenderId, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecution);
}

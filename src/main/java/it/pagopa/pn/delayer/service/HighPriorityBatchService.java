package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public interface HighPriorityBatchService {

    Mono<Void> initHighPriorityBatch(String pk, Map<String, AttributeValue> lastEvaluatedKey);
}

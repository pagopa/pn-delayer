package it.pagopa.pn.delayer.service;


import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.Map;

public interface EvaluateResidualCapacityJobService {

    Mono<Void> startEvaluateResidualCapacityJob(String unifiedDeliveryDriver, String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch, String tenderId);

}

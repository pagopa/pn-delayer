package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

import java.time.Instant;

public interface EvaluateDriverCapacityJobService {

    Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, Instant startExecutionBatch, String tenderId);
}

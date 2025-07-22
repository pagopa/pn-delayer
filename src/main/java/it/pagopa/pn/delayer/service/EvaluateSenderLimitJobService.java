package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

import java.time.Instant;

public interface EvaluateSenderLimitJobService {

    Mono<Void> startSenderLimitJob(String province, String tenderId, Instant startExecution);
}

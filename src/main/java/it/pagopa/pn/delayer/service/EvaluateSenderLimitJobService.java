package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

import java.time.LocalDate;

public interface EvaluateSenderLimitJobService {

    Mono<Void> startSenderLimitJob(String province, String tenderId, LocalDate deliveryWeek);
}

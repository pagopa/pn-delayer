package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

import java.time.LocalDate;

public interface EvaluateSenderPriorityJobService {

    Mono<Void> startSenderPriorityJob(String paId, LocalDate deliveryWeek);

}

package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

import java.time.LocalDate;

public interface EvaluateDriverCapacityJobService {

    Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, LocalDate deliveryWeek, String tenderId);
}

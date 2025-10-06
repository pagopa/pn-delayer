package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

import java.time.LocalDate;

public interface EvaluateResidualCapacityJobService {

    Mono<Void> startEvaluateResidualCapacityJob(String unifiedDeliveryDriver, String province, LocalDate deliveryWeek, String tenderId);

}

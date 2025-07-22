package it.pagopa.pn.delayer.middleware.dao;

import reactor.core.publisher.Mono;

import java.time.LocalDate;

public interface PaperDeliveryPrintCapacityDAO {

    Mono<Integer> retrieveActualPrintCapacity(LocalDate deliveryWeek);

}

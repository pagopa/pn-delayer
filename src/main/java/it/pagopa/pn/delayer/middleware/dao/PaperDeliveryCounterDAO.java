package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;

public interface PaperDeliveryCounterDAO {

    Mono<List<PaperDeliveryCounter>> getPaperDeliveryCounter(LocalDate deliveryDate, String sk);

    Mono<Void> updatePrintCapacityCounter(LocalDate deliveryDate, Integer counter, Integer weeklyPrintCapacity, Integer excludedDeliveryCounter);


}

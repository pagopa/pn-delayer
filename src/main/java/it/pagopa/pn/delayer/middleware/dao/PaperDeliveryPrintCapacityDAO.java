package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacityCounter;
import it.pagopa.pn.delayer.model.PrintCapacityEnum;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;

public interface PaperDeliveryPrintCapacityDAO {

    Mono<Integer> retrieveActualPrintCapacity(LocalDate deliveryWeek);

    Mono<List<PaperDeliveryPrintCapacityCounter>> retrievePrintCapacityCounters(LocalDate deliveryWeek, LocalDate today);

    Mono<Void> updatePrintCapacity(PrintCapacityEnum printCapacityEnum, LocalDate deliveryWeek, Integer increment, Integer printCapacity);

}

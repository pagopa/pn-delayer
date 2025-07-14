package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import reactor.core.publisher.Mono;

public interface PaperDeliveryCounterDAO {

    Mono<PaperDeliveryCounter> getPaperDeliveryCounter(String pk, String sk);

    Mono<Void> updatePrintCapacityCounter(String deliveryDate, Integer counter, Integer weeklyPrintCapacity, Integer excludedDeliveryCounter);


}

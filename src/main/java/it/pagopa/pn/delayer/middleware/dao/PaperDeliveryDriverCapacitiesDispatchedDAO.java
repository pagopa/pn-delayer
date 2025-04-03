package it.pagopa.pn.delayer.middleware.dao;


import reactor.core.publisher.Mono;

public interface PaperDeliveryDriverCapacitiesDispatchedDAO {

    Mono<Boolean> update(String deliveryDriverId, String geoKey, String tenderId, Integer increment);

    Mono<Integer> get(String deliveryDriverId, String geoKey);

}

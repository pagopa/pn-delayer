package it.pagopa.pn.delayer.middleware.dao;


import reactor.core.publisher.Mono;

public interface PaperDeliveryDriverCapacitiesDAO {

    Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geoKey);

}

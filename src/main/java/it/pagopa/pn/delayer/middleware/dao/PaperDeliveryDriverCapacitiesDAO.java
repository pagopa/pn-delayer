package it.pagopa.pn.delayer.middleware.dao;


import reactor.core.publisher.Mono;

import java.time.Instant;

public interface PaperDeliveryDriverCapacitiesDAO {

    Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String unifiedDeliveryDriver, String geoKey, Instant deliveryDate);

}

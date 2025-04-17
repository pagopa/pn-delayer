package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

public interface PaperDeliveryDriverUsedCapacitiesDAO {

    Mono<Integer> updateCounter(String unifiedDeliveryDriver, String geoKey, Integer increment, Instant deliveryDate);

    Mono<Integer> get(String unifiedDeliveryDriver, String geoKey, Instant deliveryDate);

    Flux<PaperDeliveryDriverUsedCapacities> batchGetItem(List<String> pks, Instant deliveryDate);

}

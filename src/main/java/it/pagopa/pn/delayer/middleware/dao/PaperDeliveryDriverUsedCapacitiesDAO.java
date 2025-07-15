package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.LocalDate;
import java.util.List;

public interface PaperDeliveryDriverUsedCapacitiesDAO {

    Mono<Integer> updateCounter(String unifiedDeliveryDriver, String geoKey, Integer increment, LocalDate deliveryDate, Integer declaredCapacity);

    Mono<Tuple2<Integer, Integer>> get(String unifiedDeliveryDriver, String geoKey, LocalDate deliveryDate);

    Flux<PaperDeliveryDriverUsedCapacities> batchGetItem(List<String> pks, LocalDate deliveryDate);

}

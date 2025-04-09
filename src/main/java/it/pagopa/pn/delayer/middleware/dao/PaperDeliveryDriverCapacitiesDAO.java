package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface PaperDeliveryDriverCapacitiesDAO {

    Mono<PaperDeliveryDriverCapacity> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geokey, Instant deliveryDate);
}

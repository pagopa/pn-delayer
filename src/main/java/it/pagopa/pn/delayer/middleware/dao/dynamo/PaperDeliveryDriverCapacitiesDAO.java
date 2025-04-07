package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacities;
import reactor.core.publisher.Mono;

public interface PaperDeliveryDriverCapacitiesDAO {

    Mono<PaperDeliveryDriverCapacities> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geokey);
}

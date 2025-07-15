package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;

public interface PaperDeliveryDriverCapacitiesDAO {

    Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String unifiedDeliveryDriver, String geoKey, LocalDate deliveryDate);

    Mono<List<PaperDeliveryDriverCapacity>> retrieveUnifiedDeliveryDriversOnProvince(String tenderId, String geoKey, LocalDate deliveryDate);
}

package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.time.Instant;
import java.util.List;

public interface PaperDeliveryDriverCapacitiesDispatchedDAO {

    String IMPLEMENTATION_TYPE_PROPERTY_NAME = "pn.delayer.storage.impl";


    Mono<UpdateItemResponse> updateCounter(String deliveryDriverId, String geoKey, Integer increment);

    Mono<Integer> get(String deliveryDriverId, String geoKey);

    Flux<PaperDeliveryDriverCapacitiesDispatched> batchGetItem(List<String> pks, Instant deliveryDate);

}

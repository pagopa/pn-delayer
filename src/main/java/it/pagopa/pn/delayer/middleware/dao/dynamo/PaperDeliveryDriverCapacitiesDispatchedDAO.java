package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.time.Instant;
import java.util.List;

public interface PaperDeliveryDriverCapacitiesDispatchedDAO {

    Mono<UpdateItemResponse> update(String pk, Instant deliveryDate, Integer increment);

    Mono<PaperDeliveryDriverCapacitiesDispatched> get(String pk, Instant deliveryDate);

    Flux<PaperDeliveryDriverCapacitiesDispatched> batchGetItem(List<String> pks, Instant deliveryDate);
}

package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveriesSenderLimit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;


public interface PaperDeliveriesSenderLimitDAO {

    Flux<PaperDeliveriesSenderLimit> batchGetItem(List<String> pks, Instant deliveryDate);

    Mono<Integer> updatePercentageLimit(String pk, Integer increment, Instant deliveryDate);
}

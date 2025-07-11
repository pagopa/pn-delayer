package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;


public interface PaperDeliverySenderLimitDAO {

    Flux<PaperDeliverySenderLimit> retrieveSendersLimit(List<String> pks, String deliveryDate);

    Mono<Integer> updateUsedSenderLimit(String pk, Integer increment, String deliveryDate, Integer senderLimit);
}

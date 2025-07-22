package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;


public interface PaperDeliverySenderLimitDAO {

    Flux<PaperDeliverySenderLimit> retrieveSendersLimit(List<String> pks, LocalDate deliveryDate);

    Mono<Long> updateUsedSenderLimit(String pk, Long increment, LocalDate deliveryDate, Integer senderLimit);

    Flux<PaperDeliveryUsedSenderLimit> retrieveUsedSendersLimit(List<String> pks, LocalDate deliveryDate);
}

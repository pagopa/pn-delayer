package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import reactor.core.publisher.Mono;

import java.util.List;

public interface PaperDeliveryReadyToSendDAO {

    List<PaperDeliveryReadyToSend> getByDeliveryDate(String deliveryDate);

    Mono<Integer> insert(List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend);
}

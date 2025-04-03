package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryReadyToSend;
import reactor.core.publisher.Mono;

import java.util.List;

public interface PaperDeliveryReadyToSendDAO {

    Mono<Integer> executeTransaction(List<PaperDeliveryHighPriority> paperDeliveryReadyToSend);

    List<PaperDeliveryReadyToSend> getByDeliveryDate(String deliveryDate);
}

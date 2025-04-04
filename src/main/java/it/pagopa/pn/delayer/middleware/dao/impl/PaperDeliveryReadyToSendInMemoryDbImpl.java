package it.pagopa.pn.delayer.middleware.dao.impl;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryReadyToSendDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryReadyToSend;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class PaperDeliveryReadyToSendInMemoryDbImpl implements PaperDeliveryReadyToSendDAO {
    private final ConcurrentHashMap<String, PaperDeliveryReadyToSend> data = new ConcurrentHashMap<>();

    @Override
    public Mono<Integer> insert(List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend) {
        paperDeliveryReadyToSend.forEach(readyToSend -> data.put(readyToSend.getDeliveryDate() + "##" + readyToSend.getRequestId(), readyToSend));
        return Mono.just(paperDeliveryReadyToSend.size());
    }
    @Override
    public List<PaperDeliveryReadyToSend> getByDeliveryDate(String deliveryDate) {
        return data.values().stream()
                .filter(readyToSend -> readyToSend.getDeliveryDate().isBefore(Instant.parse(deliveryDate)))
                .toList();
    }

}

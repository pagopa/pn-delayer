package it.pagopa.pn.delayer.middleware.dao.inmemory;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class PaperDeliveryReadyToSendInMemoryDbImpl {
    private final ConcurrentHashMap<String, PaperDeliveryReadyToSend> data = new ConcurrentHashMap<>();

    public Mono<Integer> insert(List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend) {
        paperDeliveryReadyToSend.forEach(readyToSend -> data.put(readyToSend.getDeliveryDate() + "##" + readyToSend.getRequestId(), readyToSend));
        return Mono.just(paperDeliveryReadyToSend.size());
    }
    public List<PaperDeliveryReadyToSend> getByDeliveryDate(Instant deliveryDate) {
        return data.values().stream()
                .filter(readyToSend -> readyToSend.getDeliveryDate().equals(deliveryDate))
                .toList();
    }

}

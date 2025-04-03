package it.pagopa.pn.delayer.middleware.dao.impl;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryReadyToSendDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class PaperDeliveryReadyToSendInMemoryDbImpl implements PaperDeliveryReadyToSendDAO {

    private final PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDAO;

    private final PaperDeliveryUtils paperDeliveryUtils;
    private final ConcurrentHashMap<String, PaperDeliveryReadyToSend> data = new ConcurrentHashMap<>();


    private Mono<Integer> insert(List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend) {
        paperDeliveryReadyToSend.forEach(readyToSend -> data.put("PK##" + readyToSend.getCreatedAt(), readyToSend));
        return Mono.just(paperDeliveryReadyToSend.size());
    }

    @Override
    public Mono<Integer> executeTransaction(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities) {
        List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend = paperDeliveryUtils.mapToPaperDeliveryReadyToSend(paperDeliveryHighPriorities);
        return insert(paperDeliveryReadyToSend)
                .flatMap(savedItems -> paperDeliveryHighPriorityDAO.delete(paperDeliveryHighPriorities.get(0).getPk(),paperDeliveryHighPriorities));
    }

    @Override
    public List<PaperDeliveryReadyToSend> getByDeliveryDate(String deliveryDate) {
        return data.values().stream()
                .filter(readyToSend -> readyToSend.getCreatedAt().isBefore(Instant.parse(deliveryDate)))
                .toList();
    }

}

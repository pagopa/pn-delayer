package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class HighPriorityServiceImpl{

    private final PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDispatchedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;

    public Mono<Tuple2<Integer, Integer>> retrieveCapacities(String geoKey, String deliveryDriverId, String tenderId, Instant deliveryWeek) {
        return paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryWeek)
                .zipWith(paperDeliveryDispatchedCapacityDAO.get(deliveryDriverId, geoKey, deliveryWeek));
    }
}

package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HighPriorityServiceImplTest {

    @Mock
    private PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;

    @Mock
    private PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDispatchedCapacityDAO;

    @InjectMocks
    private HighPriorityServiceImpl highPriorityService;

    @Test
    void retrieveCapacities_returnsCorrectCapacities() {
        String geoKey = "geo1";
        String deliveryDriverId = "driver1";
        String tenderId = "tender1";
        Instant deliveryWeek = Instant.now();

        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryWeek))
                .thenReturn(Mono.just(100));
        when(paperDeliveryDispatchedCapacityDAO.get(deliveryDriverId, geoKey, deliveryWeek))
                .thenReturn(Mono.just(50));

        Tuple2<Integer, Integer> result = highPriorityService.retrieveCapacities(geoKey, deliveryDriverId, tenderId, deliveryWeek).block();

        assertEquals(100, result.getT1());
        assertEquals(50, result.getT2());
    }
}
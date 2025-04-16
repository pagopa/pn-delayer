package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImplTest {

    PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl dao;

    @Mock
    PaperDeliveryUtils paperDeliveryUtils;

    @BeforeEach
    void setUp() throws IOException {
        dao = new PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl(paperDeliveryUtils, new ObjectMapper());
    }


    @Test
    void updateCounter_incrementsExistingCapacity() {
        String deliveryDriverId = "5";
        String geoKey = "90010";

        dao.updateCounter(deliveryDriverId, geoKey, 5, null).block();

        Mono<Integer> result = dao.get(deliveryDriverId, geoKey, null);

        Assertions.assertEquals(605, result.block());
    }

    @Test
    void updateCounter_createsNewCapacityIfNotExists() {
        String deliveryDriverId = "driver2";
        String geoKey = "geo2";
        Instant deliveryDate = Instant.now();

        when(paperDeliveryUtils.calculateNextWeek(any())).thenReturn(deliveryDate);

        Mono<Integer> response = dao.updateCounter(deliveryDriverId, geoKey, 10, deliveryDate);

        Assertions.assertEquals(10, dao.get(deliveryDriverId, geoKey, deliveryDate).block());
        Assertions.assertEquals(10, response.block());
    }

    @Test
    void returnsCorrectUsedCapacityForExistingEntry() {
        String deliveryDriverId = "driver3";
        String geoKey = "geo3";
        Instant deliveryDate = Instant.now();
        dao.updateCounter(deliveryDriverId, geoKey, 5, deliveryDate).block();

        Integer result = dao.get(deliveryDriverId, geoKey, deliveryDate).block();

        Assertions.assertEquals(5, result);
    }

    @Test
    void get_returnsZeroForNonExistingEntry() {
        String deliveryDriverId = "driver4";
        String geoKey = "geo4";
        Instant deliveryDate = Instant.now();

        Integer result = dao.get(deliveryDriverId, geoKey, deliveryDate).block();

        Assertions.assertEquals(0, result);
    }
}
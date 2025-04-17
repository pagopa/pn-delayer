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
class PaperDeliveryDriverUsedCapacitiesInMemoryDbImplTest {

    PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl dao;

    @Mock
    PaperDeliveryUtils paperDeliveryUtils;

    @BeforeEach
    void setUp() throws IOException {
        dao = new PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl(paperDeliveryUtils, new ObjectMapper());
    }


    @Test
    void updateCounter_incrementsExistingCapacity() {
        String unifiedDeliveryDriver = "5";
        String geoKey = "90010";

        dao.updateCounter(unifiedDeliveryDriver, geoKey, 5, null).block();

        Mono<Integer> result = dao.get(unifiedDeliveryDriver, geoKey, null);

        Assertions.assertEquals(605, result.block());
    }

    @Test
    void updateCounter_createsNewCapacityIfNotExists() {
        String unifiedDeliveryDriver = "driver2";
        String geoKey = "geo2";
        Instant deliveryDate = Instant.now();

        when(paperDeliveryUtils.calculateNextWeek(any())).thenReturn(deliveryDate);

        Mono<Integer> response = dao.updateCounter(unifiedDeliveryDriver, geoKey, 10, deliveryDate);

        Assertions.assertEquals(10, dao.get(unifiedDeliveryDriver, geoKey, deliveryDate).block());
        Assertions.assertEquals(10, response.block());
    }

    @Test
    void returnsCorrectUsedCapacityForExistingEntry() {
        String unifiedDeliveryDriver = "driver3";
        String geoKey = "geo3";
        Instant deliveryDate = Instant.now();
        dao.updateCounter(unifiedDeliveryDriver, geoKey, 5, deliveryDate).block();

        Integer result = dao.get(unifiedDeliveryDriver, geoKey, deliveryDate).block();

        Assertions.assertEquals(5, result);
    }

    @Test
    void get_returnsZeroForNonExistingEntry() {
        String unifiedDeliveryDriver = "driver4";
        String geoKey = "geo4";
        Instant deliveryDate = Instant.now();

        Integer result = dao.get(unifiedDeliveryDriver, geoKey, deliveryDate).block();

        Assertions.assertEquals(0, result);
    }
}
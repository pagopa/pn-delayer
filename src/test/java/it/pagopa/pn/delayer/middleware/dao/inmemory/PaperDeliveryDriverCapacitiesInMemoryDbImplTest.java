package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Instant;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryDriverCapacitiesInMemoryDbImplTest {

    PaperDeliveryDriverCapacitiesInMemoryDbImpl dao;

    @BeforeEach
    void setUp() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        dao = new PaperDeliveryDriverCapacitiesInMemoryDbImpl(objectMapper);
    }

    @Test
    void returnsCorrectCapacityForMatchingEntry() {
        String tenderId = "c0d82f6e-ee85-4e27-97df-bef27c5c5377";
        String deliveryDriverId = "1";
        String geoKey = "61010";
        Instant deliveryDate = Instant.parse("2025-01-01T00:00:00.000Z");
        Integer result = dao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryDate).block();

        Assertions.assertEquals(5000, result);
    }

    @Test
    void returnsZeroWhenNoMatchingEntryExists() {
        String tenderId = "tender2";
        String deliveryDriverId = "driver2";
        String geoKey = "geo2";
        Instant deliveryDate = Instant.parse("2023-10-01T10:00:00Z");

        Integer result = dao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryDate).block();

        Assertions.assertEquals(0, result);
    }

    @Test
    void returnsZeroWhenDeliveryDateIsOutsideActivationRange() {
        String tenderId = "c0d82f6e-ee85-4e27-97df-bef27c5c5377";
        String deliveryDriverId = "1";
        String geoKey = "61010";
        Instant deliveryDate = Instant.parse("2024-01-01T00:00:00.000Z");

        Integer result = dao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryDate).block();
        Assertions.assertEquals(0, result);
    }

    @Test
    void returnsCapacityForEntryWithValidCloseInterval() {
        String tenderId = "c0d82f6e-ee85-4e27-97df-bef27c5c5377";
        String deliveryDriverId = "4";
        String geoKey = "MI";
        Instant deliveryDate = Instant.parse("2025-04-01T00:00:00.000Z");

        Integer result = dao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryDate).block();

        Assertions.assertEquals(5000, result);
    }

    @Test
    void returnsCapacityForEntryWithInvalidCloseInterval() {
        String tenderId = "c0d82f6e-ee85-4e27-97df-bef27c5c5377";
        String deliveryDriverId = "3";
        String geoKey = "GR";
        Instant deliveryDate = Instant.parse("2025-05-01T00:00:00.000Z");

        Integer result = dao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geoKey, deliveryDate).block();

        Assertions.assertEquals(1000, result);
    }
}

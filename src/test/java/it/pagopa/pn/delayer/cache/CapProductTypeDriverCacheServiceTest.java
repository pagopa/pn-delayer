package it.pagopa.pn.delayer.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class CapProductTypeDriverCacheServiceTest {

    @InjectMocks
    private CapProductTypeDriverCacheService capProductTypeDriverCacheService;

    @Test
    void addToCache_storesValueCorrectly() {
        String key = "ProductA~Cap1";
        String value = "Driver1";

        capProductTypeDriverCacheService.addToCache(key, value);

        Optional<String> result = capProductTypeDriverCacheService.getFromCache(key);
        assertTrue(result.isPresent());
        assertEquals("Driver1", result.get());
    }

    @Test
    void getFromCache_returnsEmptyOptionalForNonExistentKey() {
        String key = "NonExistentKey";

        Optional<String> result = capProductTypeDriverCacheService.getFromCache(key);

        assertTrue(result.isEmpty());
    }

    @Test
    void getFromCache_returnsCorrectValueForExistingKey() {
        String key = "ProductB~Cap2";
        String value = "Driver2";

        capProductTypeDriverCacheService.addToCache(key, value);

        Optional<String> result = capProductTypeDriverCacheService.getFromCache(key);
        assertTrue(result.isPresent());
        assertEquals("Driver2", result.get());
    }
}

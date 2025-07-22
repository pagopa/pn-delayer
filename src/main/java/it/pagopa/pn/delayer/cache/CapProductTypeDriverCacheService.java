package it.pagopa.pn.delayer.cache;

import lombok.extern.slf4j.Slf4j;
import net.jodah.expiringmap.ExpiringMap;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class CapProductTypeDriverCacheService {

    protected ExpiringMap<String, String> capProductDriverCache = ExpiringMap.builder()
            .expiration(1, TimeUnit.HOURS)
            .asyncExpirationListener((key, driver) -> log.info("Cache entry for key {} - driver {}", key, driver))
            .variableExpiration()
            .build();

    public void addToCache(String key, String value) {
        capProductDriverCache.put(key, value);
    }

    public Optional<String> getFromCache(String capProductTypeKey) {
        return Optional.ofNullable(capProductDriverCache.get(capProductTypeKey));
    }
}
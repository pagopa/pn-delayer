package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import it.pagopa.pn.delayer.model.ImplementationType;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.INMEMORY)
public class PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl implements PaperDeliveryDriverUsedCapacitiesDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverUsedCapacities> data = new ConcurrentHashMap<>();
    private final PaperDeliveryUtils paperDeliveryUtils;

    public PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl(PaperDeliveryUtils paperDeliveryUtils, ObjectMapper objectMapper) throws IOException {
        this.paperDeliveryUtils = paperDeliveryUtils;
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryDriverCapacitiesUsed.json");
        List<PaperDeliveryDriverUsedCapacities> capacities = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {
        });
        Instant deliveryDte = paperDeliveryUtils.calculateDeliveryWeek(Instant.now());
        capacities.forEach(used -> used.setDeliveryDate(deliveryDte));
        capacities.forEach(used -> data.put(used.getUnifiedDeliveryDriverGeokey() + "~" + used.getDeliveryDate(), used));
        log.info("loaded {} PaperDeliveryDriverCapacitiesUsed with deliveryDate: {}", capacities.size(), deliveryDte);
    }

    @Override
    public Mono<Integer> updateCounter(String unifiedDeliveryDriver, String geoKey, Integer increment, Instant deliveryDate) {
        String mapPk = constructPk(unifiedDeliveryDriver + "~" + geoKey, deliveryDate);
        data.merge(mapPk, createNewCapacity(unifiedDeliveryDriver, geoKey, increment),
                (existingValue, newValue) -> {
                    existingValue.setUsedCapacity(existingValue.getUsedCapacity() + increment);
                    return existingValue;
                });
        log.info("updated PaperDeliveryDriverCapacitiesUsed with unifiedDeliveryDriver: {}, geoKey: {}, increment: {}, deliveryDate: {}",
                unifiedDeliveryDriver, geoKey, increment, deliveryDate);
        return Mono.just(increment);
    }

    private PaperDeliveryDriverUsedCapacities createNewCapacity(String unifiedDeliveryDriver, String geoKey, Integer increment) {
        PaperDeliveryDriverUsedCapacities capacity = new PaperDeliveryDriverUsedCapacities();
        capacity.setUnifiedDeliveryDriverGeokey(unifiedDeliveryDriver + "~" + geoKey);
        capacity.setUnifiedDeliveryDriver(unifiedDeliveryDriver);
        capacity.setGeoKey(geoKey);
        capacity.setUsedCapacity(increment);
        capacity.setDeliveryDate(paperDeliveryUtils.calculateDeliveryWeek(Instant.now()));
        return capacity;
    }

    @Override
    public Mono<Integer> get(String unifiedDeliveryDriver, String geoKey, Instant deliveryDate) {
        String pk = unifiedDeliveryDriver + "~" + geoKey;
        return Mono.just(Optional.ofNullable(data.get(constructPk(pk, deliveryDate)))
                .map(paperDeliveryDriverUsedCapacities -> {
                    log.info("retrieved PaperDeliveryDriverCapacitiesUsed with unifiedDeliveryDriver: {}, geoKey: {}, deliveryDate: {}",
                            unifiedDeliveryDriver, geoKey, deliveryDate);
                    return paperDeliveryDriverUsedCapacities.getUsedCapacity();
                })
                .orElseGet(() -> {
                    log.info("PaperDeliveryDriverCapacitiesUsed not found with unifiedDeliveryDriver: {}, geoKey: {}, deliveryDate: {}, returning 0",
                            unifiedDeliveryDriver, geoKey, deliveryDate);
                    return 0;
                }));
    }

    @Override
    public Flux<PaperDeliveryDriverUsedCapacities> batchGetItem(List<String> pks, Instant deliveryDate) {
        return null;
    }

    public String constructPk(String pk, Instant deliveryDate) {
        return pk + "~" + deliveryDate;
    }
}

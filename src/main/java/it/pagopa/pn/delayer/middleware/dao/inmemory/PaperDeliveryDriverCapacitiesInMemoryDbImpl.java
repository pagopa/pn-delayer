package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.ImplementationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.INMEMORY)
public class PaperDeliveryDriverCapacitiesInMemoryDbImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverCapacity> data = new ConcurrentHashMap<>();

    public PaperDeliveryDriverCapacitiesInMemoryDbImpl(ObjectMapper objectMapper) throws IOException {
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryDriverCapacities.json");
        List<PaperDeliveryDriverCapacity> capacityList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {
        });
        data.putAll(capacityList.stream().collect(Collectors.toMap(this::constructKey, item -> item)));
        log.info("loaded {} PaperDeliveryDriverCapacities", capacityList.size());
    }


    @Override
    public Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String unifiedDeliveryDriver, String geoKey, Instant deliveryDate) {
        return Mono.just(data.values()
                .stream()
                .filter(driverCapacities -> driverCapacities.getUnifiedDeliveryDriver().equalsIgnoreCase(unifiedDeliveryDriver)
                        && driverCapacities.getGeoKey().equalsIgnoreCase(geoKey)
                        && driverCapacities.getTenderId().equalsIgnoreCase(tenderId)
                        && (deliveryDate.equals(driverCapacities.getActivationDateFrom()) || deliveryDate.isAfter(driverCapacities.getActivationDateFrom()))
                        && (driverCapacities.getActivationDateTo() == null || deliveryDate.isBefore(driverCapacities.getActivationDateTo())))
                .min((o1, o2) -> o2.getActivationDateFrom().compareTo(o1.getActivationDateFrom()))
                .map(paperDeliveryDriverCapacity -> {
                    log.info("found PaperDeliveryDriverCapacities with unifiedDeliveryDriver: {}, geoKey: {}, tenderId: {}, deliveryDate: {}",
                            unifiedDeliveryDriver, geoKey, tenderId, deliveryDate);
                    return paperDeliveryDriverCapacity.getCapacity();
                })
                .orElseGet(() -> {
                    log.info("PaperDeliveryDriverCapacities not found with unifiedDeliveryDriver: {}, geoKey: {}, tenderId: {}, deliveryDate: {} returning 0",
                            unifiedDeliveryDriver, geoKey, tenderId, deliveryDate);
                    return 0;
                }));
    }

    private String constructKey(PaperDeliveryDriverCapacity capacity) {
        return capacity.getPk() + "~" + capacity.getActivationDateFrom();
    }
}

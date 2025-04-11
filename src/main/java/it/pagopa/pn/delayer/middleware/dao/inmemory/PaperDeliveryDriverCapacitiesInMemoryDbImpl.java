package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.ImplementationType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.INMEMORY)
public class PaperDeliveryDriverCapacitiesInMemoryDbImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverCapacity> data = new ConcurrentHashMap<>();

    public PaperDeliveryDriverCapacitiesInMemoryDbImpl() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryDriverCapacities.json");
        List<PaperDeliveryDriverCapacity> capacityList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {
        });
        data.putAll(capacityList.stream().collect(Collectors.toMap(this::constructKey, item -> item)));
    }


    @Override
    public Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geoKey, Instant deliveryDate) {
        return Mono.just(data.values()
                .stream()
                .filter(driverCapacities -> driverCapacities.getDeliveryDriverId().equalsIgnoreCase(deliveryDriverId)
                        && driverCapacities.getGeoKey().equalsIgnoreCase(geoKey)
                        && driverCapacities.getTenderId().equalsIgnoreCase(tenderId)
                        && deliveryDate.isAfter(driverCapacities.getActivationDateFrom())
                        && (driverCapacities.getActivationDateTo() == null || deliveryDate.isBefore(driverCapacities.getActivationDateTo())))
                .min((o1, o2) -> o2.getActivationDateFrom().compareTo(o1.getActivationDateFrom()))
                .map(PaperDeliveryDriverCapacity::getCapacity)
                .orElse(0));
    }

    private String constructKey(PaperDeliveryDriverCapacity capacity) {
        return capacity.getPk() + "##" + capacity.getActivationDateFrom();
    }

    public Collection<PaperDeliveryDriverCapacity> getAll() {
        return data.values();
    }
}

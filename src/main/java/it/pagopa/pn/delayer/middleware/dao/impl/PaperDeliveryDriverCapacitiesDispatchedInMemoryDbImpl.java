package it.pagopa.pn.delayer.middleware.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryDriverCapacitiesDispatched;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl implements PaperDeliveryDriverCapacitiesDispatchedDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverCapacitiesDispatched> data = new ConcurrentHashMap<>();
    private final PaperDeliveryUtils paperDeliveryUtils;

    public PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl(PaperDeliveryUtils paperDeliveryUtils) throws IOException {
        this.paperDeliveryUtils = paperDeliveryUtils;
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryDriverCapacitiesDispatched.json");
        List<PaperDeliveryDriverCapacitiesDispatched> capacities = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {});
        capacities.forEach(dispatched -> dispatched.setDeliveryDate(paperDeliveryUtils.calculateNextWeek(Instant.now())));
        capacities.forEach(dispatched -> data.put(dispatched.getPk() + "##" + dispatched.getDeliveryDate(), dispatched));
    }

    @Override
    public Mono<Boolean> update(String deliveryDriverId, String geoKey, String tenderId, Integer increment) {
        String pk = constructPk(deliveryDriverId, geoKey);
        data.merge(pk, createNewCapacity(deliveryDriverId, geoKey, tenderId, increment),
                (existingValue, newValue) -> {
                    existingValue.setCapacity(existingValue.getCapacity() + increment);
                    return existingValue;
                });
        return Mono.just(true);
    }

    private PaperDeliveryDriverCapacitiesDispatched createNewCapacity(String deliveryDriverId, String geoKey, String tenderId, Integer increment) {
        PaperDeliveryDriverCapacitiesDispatched capacity = new PaperDeliveryDriverCapacitiesDispatched();
        capacity.setPk(deliveryDriverId + "##" + geoKey);
        capacity.setDeliveryDriverId(deliveryDriverId);
        capacity.setGeoKey(geoKey);
        capacity.setTenderId(tenderId);
        capacity.setCapacity(increment);
        capacity.setDeliveryDate(paperDeliveryUtils.calculateNextWeek(Instant.now()));
        return capacity;
    }

    @Override
    public Mono<Integer> get(String deliveryDriverId, String geoKey) {
        return Mono.just(Optional.ofNullable(data.get(constructPk(deliveryDriverId, geoKey)))
                .map(PaperDeliveryDriverCapacitiesDispatched::getCapacity)
                .orElse(0));
    }


    public Collection<PaperDeliveryDriverCapacitiesDispatched> getAll() {
        return data.values();
    }

    public String constructPk(String deliveryDriverId, String geoKey) {
        return deliveryDriverId + "##" + geoKey + "##" + paperDeliveryUtils.calculateNextWeek(Instant.now());
    }
}

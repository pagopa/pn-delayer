package it.pagopa.pn.delayer.middleware.dao.impl;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryDriverCapacitiesDispatched;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl implements PaperDeliveryDriverCapacitiesDispatchedDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverCapacitiesDispatched> data = new ConcurrentHashMap<>();
    private final PaperDeliveryUtils paperDeliveryUtils;

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
        capacity.setDeliveryDate(paperDeliveryUtils.calculateNextWeek(Instant.now().toString()));
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

    private String constructKey(PaperDeliveryDriverCapacitiesDispatched capacity) {
        return capacity.getPk() + "##" + capacity.getDeliveryDate();
    }

    public String constructPk(String deliveryDriverId, String geoKey) {
        return deliveryDriverId + "##" + geoKey + "##" + paperDeliveryUtils.calculateNextWeek(Instant.now().toString());
    }
}

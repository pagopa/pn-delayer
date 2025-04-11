package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.pagopa.pn.commons.abstractions.impl.MiddlewareTypes;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import it.pagopa.pn.delayer.model.ImplementationType;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.INMEMORY)
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
        capacities.forEach(dispatched -> data.put(dispatched.getDeliveryDriverIdGeokey() + "##" + dispatched.getDeliveryDate(), dispatched));
    }

    @Override
    public Mono<UpdateItemResponse> updateCounter(String deliveryDriverId, String geoKey, Integer increment, Instant deliveryDate) {
        String mapPk = constructPk(deliveryDriverId + "##" + geoKey, deliveryDate);
        data.merge(mapPk, createNewCapacity(deliveryDriverId, geoKey, increment),
                (existingValue, newValue) -> {
                    existingValue.setUsedCapacity(existingValue.getUsedCapacity() + increment);
                    return existingValue;
                });
        return Mono.just(UpdateItemResponse.builder().build());
    }

    private PaperDeliveryDriverCapacitiesDispatched createNewCapacity(String deliveryDriverId, String geoKey,  Integer increment) {
        PaperDeliveryDriverCapacitiesDispatched capacity = new PaperDeliveryDriverCapacitiesDispatched();
        capacity.setDeliveryDriverIdGeokey(deliveryDriverId + "##" + geoKey);
        capacity.setDeliveryDriverId(deliveryDriverId);
        capacity.setGeoKey(geoKey);
        capacity.setUsedCapacity(increment);
        capacity.setDeliveryDate(paperDeliveryUtils.calculateNextWeek(Instant.now()));
        return capacity;
    }

    @Override
    public Mono<Integer> get(String deliveryDriverId, String geoKey, Instant deliveryDate) {
        String pk = deliveryDriverId + "##" + geoKey;
        return Mono.just(Optional.ofNullable(data.get(constructPk(pk, deliveryDate)))
                .map(PaperDeliveryDriverCapacitiesDispatched::getUsedCapacity)
                .orElse(0));
    }

    @Override
    public Flux<PaperDeliveryDriverCapacitiesDispatched> batchGetItem(List<String> pks, Instant deliveryDate) {
        return null;
    }


    public Collection<PaperDeliveryDriverCapacitiesDispatched> getAll() {
        return data.values();
    }

    public String constructPk(String pk, Instant deliveryDate) {
        return pk + "##" + deliveryDate;
    }
}

package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import it.pagopa.pn.delayer.model.ImplementationType;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.INMEMORY)
public class PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl implements PaperDeliveryDriverCapacitiesDispatchedDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverCapacitiesDispatched> data = new ConcurrentHashMap<>();
    private final PaperDeliveryUtils paperDeliveryUtils;

    public PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl(PaperDeliveryUtils paperDeliveryUtils, ObjectMapper objectMapper) throws IOException {
        this.paperDeliveryUtils = paperDeliveryUtils;
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryDriverCapacitiesDispatched.json");
        List<PaperDeliveryDriverCapacitiesDispatched> capacities = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {
        });
        Instant deliveryDte = paperDeliveryUtils.calculateNextWeek(Instant.now());
        capacities.forEach(dispatched -> dispatched.setDeliveryDate(deliveryDte));
        capacities.forEach(dispatched -> data.put(dispatched.getDeliveryDriverIdGeokey() + "##" + dispatched.getDeliveryDate(), dispatched));
        log.info("loaded {} PaperDeliveryDriverCapacitiesDispatched with deliveryDate: {}", capacities.size(), deliveryDte);
    }

    @Override
    public Mono<UpdateItemResponse> updateCounter(String deliveryDriverId, String geoKey, Integer increment, Instant deliveryDate) {
        String mapPk = constructPk(deliveryDriverId + "##" + geoKey, deliveryDate);
        data.merge(mapPk, createNewCapacity(deliveryDriverId, geoKey, increment),
                (existingValue, newValue) -> {
                    existingValue.setUsedCapacity(existingValue.getUsedCapacity() + increment);
                    return existingValue;
                });
        log.info("updated PaperDeliveryDriverCapacitiesDispatched with deliveryDriverId: {}, geoKey: {}, increment: {}, deliveryDate: {}",
                deliveryDriverId, geoKey, increment, deliveryDate);
        return Mono.just(UpdateItemResponse.builder().build());
    }

    private PaperDeliveryDriverCapacitiesDispatched createNewCapacity(String deliveryDriverId, String geoKey, Integer increment) {
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
                .map(paperDeliveryDriverCapacitiesDispatched -> {
                    log.info("retrieved PaperDeliveryDriverCapacitiesDispatched with deliveryDriverId: {}, geoKey: {}, deliveryDate: {}",
                            deliveryDriverId, geoKey, deliveryDate);
                    return paperDeliveryDriverCapacitiesDispatched.getUsedCapacity();
                })
                .orElseGet(() -> {
                    log.info("PaperDeliveryDriverCapacitiesDispatched not found with deliveryDriverId: {}, geoKey: {}, deliveryDate: {}, returning 0",
                            deliveryDriverId, geoKey, deliveryDate);
                    return 0;
                }));
    }

    @Override
    public Flux<PaperDeliveryDriverCapacitiesDispatched> batchGetItem(List<String> pks, Instant deliveryDate) {
        return null;
    }

    public String constructPk(String pk, Instant deliveryDate) {
        return pk + "##" + deliveryDate;
    }
}

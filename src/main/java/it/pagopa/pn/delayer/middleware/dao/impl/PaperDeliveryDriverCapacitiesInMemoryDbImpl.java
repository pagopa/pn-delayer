package it.pagopa.pn.delayer.middleware.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryDriverCapacities;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class PaperDeliveryDriverCapacitiesInMemoryDbImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final ConcurrentHashMap<String, PaperDeliveryDriverCapacities> data = new ConcurrentHashMap<>();

    public PaperDeliveryDriverCapacitiesInMemoryDbImpl() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryDriverCapacities.json");
        List<PaperDeliveryDriverCapacities> capacityList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {
        });
        data.putAll(capacityList.stream().collect(Collectors.toMap(this::constructKey, item -> item)));
    }


    @Override
    public Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geoKey) {
        Instant now = Instant.now();
        return Mono.just(data.values()
                .stream()
                .filter(driverCapacities -> driverCapacities.getDeliveryDriverId().equalsIgnoreCase(deliveryDriverId)
                        && driverCapacities.getGeoKey().equalsIgnoreCase(geoKey)
                        && driverCapacities.getTenderId().equalsIgnoreCase(tenderId)
                        && now.isAfter(driverCapacities.getActivationDateFrom())
                        && (driverCapacities.getActivationDateTo() == null || now.isBefore(driverCapacities.getActivationDateTo())))
                .min((o1, o2) -> o2.getActivationDateFrom().compareTo(o1.getActivationDateFrom()))
                .map(PaperDeliveryDriverCapacities::getCapacity)
                .orElse(0));
    }

    private String constructKey(PaperDeliveryDriverCapacities capacity) {
        return capacity.getPk() + "##" + capacity.getActivationDateFrom();
    }

    public Collection<PaperDeliveryDriverCapacities> getAll() {
        return data.values();
    }
}

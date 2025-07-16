package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.LocalDate;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class DeliveryDriverCapacityService {

    private final PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryDriverCapacitiesDAO;
    private final PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    public Mono<Tuple2<Integer, Integer>> retrieveDeclaredAndUsedCapacity(String geoKey, String unifiedDeliveryDriver, String tenderId, LocalDate deliveryWeek) {
        return paperDeliveryUsedCapacityDAO.get(unifiedDeliveryDriver, geoKey, deliveryWeek)
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No used capacities found for unifiedDeliveryDriver={}, geoKey={}, deliveryWeek={}, retrieving declared capacity", unifiedDeliveryDriver, geoKey, deliveryWeek);
                    return paperDeliveryDriverCapacitiesDAO.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geoKey, deliveryWeek)
                            .map(capacity -> Tuples.of(capacity, 0));
                }));
    }

    public Mono<Void> updateCounters(List<IncrementUsedCapacityDto> incrementCapacities) {
        return Flux.fromIterable(incrementCapacities)
                .flatMap(incrementUsedCapacityDto ->
                        paperDeliveryUsedCapacityDAO.updateCounter(
                                incrementUsedCapacityDto.unifiedDeliveryDriver(),
                                incrementUsedCapacityDto.geoKey(),
                                incrementUsedCapacityDto.numberOfDeliveries(),
                                incrementUsedCapacityDto.deliveryWeek(),
                                incrementUsedCapacityDto.declaredCapacity()))
                .then();
    }

    public Mono<DriversTotalCapacity> retrieveDriversCapacityOnProvince(LocalDate deliveryDate, String tenderId, String province) {
        return paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "EXCLUDE~" + province)
                .map(PaperDeliveryCounter::getCounter)
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No paper delivery counter found for tenderId: {}, province: {}, deliveryDate: {}", tenderId, province, deliveryDate);
                    return Mono.just(0);
                }))
                .flatMap(counter ->
                        paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate)
                                .map(driverCapacitiesList -> {
                                    int totalCapacity = driverCapacitiesList.stream().mapToInt(PaperDeliveryDriverCapacity::getCapacity).sum();
                                    int availableCapacity = totalCapacity - counter;
                                    List<String> unifiedDeliveryDrivers = driverCapacitiesList.stream().map(PaperDeliveryDriverCapacity::getUnifiedDeliveryDriver).toList();
                                    return new DriversTotalCapacity(availableCapacity, unifiedDeliveryDrivers);
                                })
                );
    }

}

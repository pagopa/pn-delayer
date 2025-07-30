package it.pagopa.pn.delayer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_DELIVERY_DRIVER_NOT_FOUND;

@Component
@Slf4j
@RequiredArgsConstructor
public class DeliveryDriverUtils {

    private final LambdaClient lambdaClient;
    private final ObjectMapper objectMapper;
    private final PnDelayerConfigs pnDelayerConfigs;
    private final CapProductTypeDriverCacheService cacheService;
    private final PnDelayerUtils pnDelayerUtils;
    private final PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;
    private final PaperDeliveryDriverCapacitiesDAO paperDeliveryDriverCapacitiesDAO;
    private final PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    public List<PaperChannelDeliveryDriver> retrieveUnifiedDeliveryDriversFromPaperChannel(List<DeliveryDriverRequest> deliveryDriverRequests, String tenderId) {
        try {
            SdkBytes sdkBytesResponse = lambdaClient.invoke(InvokeRequest.builder()
                            .functionName(pnDelayerConfigs.getPaperChannelTenderApiLambdaArn())
                            .invocationType(InvocationType.REQUEST_RESPONSE)
                            .payload(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(new PaperChannelDeliveryDriverRequest(deliveryDriverRequests, tenderId, "GET_UNIFIED_DELIVERY_DRIVERS"))))
                            .build())
                    .payload();
            return objectMapper.readValue(sdkBytesResponse.asByteArray(), PaperChannelDeliveryDriverResponse.class).getBody();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertInCache(List<PaperChannelDeliveryDriver> paperChannelDeliveryDriver) {
        Map<String, String> map = pnDelayerUtils.groupByGeoKeyAndProduct(paperChannelDeliveryDriver);
        map.forEach(cacheService::addToCache);
    }

    public Optional<String> retrieveFromCache(String capProductTypeKey) {
        return cacheService.getFromCache(capProductTypeKey);
    }

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

    public Mono<List<DriversTotalCapacity>> retrieveDriversCapacityOnProvince(LocalDate deliveryDate, String tenderId, String province) {
        return paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate.toString(), PaperDeliveryCounter.buildSkPrefix(PaperDeliveryCounter.SkPrefix.EXCLUDE, province), null)
                .defaultIfEmpty(Collections.emptyList())
                .map(this::createProductCounterMap)
                .doOnNext(stringIntegerMap -> log.info("Retrieved counters for province {}: {}", province, stringIntegerMap))
                .flatMap(counters ->
                        paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate)
                                .map(this::groupDriversByIntersectingProducts)
                                .flatMapMany(map -> Flux.fromIterable(map.entrySet()))
                                .map(entry -> {
                                    Integer capacity = entry.getValue().stream().mapToInt(PaperDeliveryDriverCapacity::getCapacity).sum();
                                    Integer reducedCapacity = capacity - counters.entrySet().stream()
                                            .filter(counter -> entry.getKey().contains(counter.getKey()))
                                            .map(Map.Entry::getValue)
                                            .reduce(0, Integer::sum);
                                    log.info("calculated reduced capacity for province {} and products [{}] --> declaredCapacity: {}, reducedCapacity: {}", province, entry.getKey(), capacity, reducedCapacity);
                                    return new DriversTotalCapacity(
                                            entry.getKey(),
                                            reducedCapacity,
                                            entry.getValue().stream().map(PaperDeliveryDriverCapacity::getUnifiedDeliveryDriver).toList()
                                    );
                                })
                                .collectList()
                                .filter(driversTotalCapacities -> !CollectionUtils.isEmpty(driversTotalCapacities))
                                .doOnDiscard(List.class, list -> {
                                    log.error("No drivers found for province: {}", province);
                                    throw new PnInternalException(String.format("No drivers found for province: %s", province),404,ERROR_CODE_DELIVERY_DRIVER_NOT_FOUND);
                                })
                );
    }

    private Map<String, Integer> createProductCounterMap(List<PaperDeliveryCounter> paperDeliveryCounters) {
        return paperDeliveryCounters.stream().collect(Collectors.toMap(paperDeliveryCounter -> PaperDeliveryCounter.retrieveProductFromSk(paperDeliveryCounter.getSk()),
                PaperDeliveryCounter::getNumberOfShipments, (existing, replacement) -> existing));
    }

    /**
     * Groups drivers by intersecting product sets.
     * If a driver has products that intersect with an existing group, the driver is merged into that group.
     * Otherwise, a new group is created for that PaperDeliveryDriverCapacity entity.
     *
     * For example, given three PaperDeliveryDriverCapacity entities with product lists: ["RS", "AR"], ["RS"], and ["890"],
     * the resulting map will have two entries with keys ["AR", "RS"] and ["890"],
     * and values containing the corresponding PaperDeliveryDriverCapacity entities.
     *
     * @param paperDeliveryDriverCapacities List of PaperDeliveryDriverCapacity objects to be grouped
     * @return A map where the key is a list of products, and the value is a list of PaperDeliveryDriverCapacity entities
     */
    public Map<List<String>, List<PaperDeliveryDriverCapacity>> groupDriversByIntersectingProducts(List<PaperDeliveryDriverCapacity> paperDeliveryDriverCapacities) {
        List<Map.Entry<List<String>, List<PaperDeliveryDriverCapacity>>> groups = new ArrayList<>();

        for (var paperDeliveryDriverCapacity : paperDeliveryDriverCapacities) {
            List<String> products = paperDeliveryDriverCapacity.getProducts();
            List<Map.Entry<List<String>, List<PaperDeliveryDriverCapacity>>> overlapping = groups.stream()
                    .filter(g -> g.getKey().stream().anyMatch(products::contains))
                    .toList();

            if (overlapping.isEmpty()) {
                groups.add(Map.entry(products, List.of(paperDeliveryDriverCapacity)));
            } else {
                List<String> mergedProducts = new ArrayList<>(products);
                List<PaperDeliveryDriverCapacity> mergedDrivers = new ArrayList<>(List.of(paperDeliveryDriverCapacity));
                overlapping.forEach(g -> {
                    mergedProducts.addAll(g.getKey());
                    mergedDrivers.addAll(g.getValue());
                });
                groups.removeAll(overlapping);
                groups.add(Map.entry(mergedProducts.stream().distinct().toList(), mergedDrivers));
            }
        }

        return groups.stream().collect(Collectors.toMap(e -> e.getKey().stream().sorted().toList(), Map.Entry::getValue));
    }
}

package it.pagopa.pn.delayer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public List<PaperChannelDeliveryDriverResponse> retrieveUnifiedDeliveryDriversFromPaperChannel(List<DeliveryDriverRequest> deliveryDriverRequests, String tenderId) {
        try {
            SdkBytes sdkBytesResponse = lambdaClient.invoke(InvokeRequest.builder()
                            .functionName(pnDelayerConfigs.getPaperChannelTenderApiLambdaName())
                            .invocationType(InvocationType.REQUEST_RESPONSE)
                            .payload(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(new PaperChannelDeliveryDriverRequest(deliveryDriverRequests, tenderId, "GET_UNIFIED_DELIVERY_DRIVERS"))))
                            .build())
                    .payload();
            return objectMapper.readValue(sdkBytesResponse.asByteArray(), new com.fasterxml.jackson.core.type.TypeReference<>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertInCache(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses) {
        Map<String, String> map = pnDelayerUtils.groupByGeoKeyAndProduct(paperChannelDeliveryDriverResponses);
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

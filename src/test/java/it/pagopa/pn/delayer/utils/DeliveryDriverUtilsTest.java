package it.pagopa.pn.delayer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DeliveryDriverUtilsTest {

    private DeliveryDriverUtils deliveryDriverUtils;

    @Mock
    private LambdaClient lambdaClient;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @Mock
    private CapProductTypeDriverCacheService cacheService;

    @Mock
    private PnDelayerUtils pnDelayerUtils;

    @Mock
    private PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryUsedCapacityDAO;

    @Mock
    private PaperDeliveryDriverCapacitiesDAO paperDeliveryDriverCapacitiesDAO;

    @Mock
    private PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    @BeforeEach
    void setUp() {
        deliveryDriverUtils = new DeliveryDriverUtils(
                lambdaClient,
                new ObjectMapper(),
                pnDelayerConfigs,
                cacheService,
                pnDelayerUtils,
                paperDeliveryUsedCapacityDAO,
                paperDeliveryDriverCapacitiesDAO,
                paperDeliveryCounterDAO
        );
    }

    @Test
    void retrieveUnifiedDeliveryDriversFromPaperChannel_handlesValidResponse() {
        List<DeliveryDriverRequest> deliveryDriverRequests = List.of(new DeliveryDriverRequest("geoKey", "AR"));
        String tenderId = "tender1";
        SdkBytes sdkBytesResponse = SdkBytes.fromUtf8String("[{\"unifiedDeliveryDriver\":\"driver1\"}]");

        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(sdkBytesResponse).build());

        List<PaperChannelDeliveryDriverResponse> result = deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(deliveryDriverRequests, tenderId);

        assertEquals(1, result.size());
        assertEquals("driver1", result.getFirst().getUnifiedDeliveryDriver());
    }

    @Test
    void retrieveUnifiedDeliveryDriversFromPaperChannel_handlesIOException() {
        List<DeliveryDriverRequest> deliveryDriverRequests = List.of(new DeliveryDriverRequest("geoKey", "AR"));
        String tenderId = "tender1";

        when(lambdaClient.invoke(any(InvokeRequest.class))).thenThrow(new RuntimeException("Error invoking Lambda"));

        assertThrows(RuntimeException.class, () -> deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(deliveryDriverRequests, tenderId));
    }

    @Test
    void retrieveDeclaredAndUsedCapacity_handlesExistingUsedCapacity() {
        String geoKey = "geo1";
        String unifiedDeliveryDriver = "driver1";
        String tenderId = "tender1";
        LocalDate deliveryWeek = LocalDate.now();

        when(paperDeliveryUsedCapacityDAO.get(unifiedDeliveryDriver, geoKey, deliveryWeek))
                .thenReturn(Mono.just(Tuples.of(100, 50)));

        StepVerifier.create(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(geoKey, unifiedDeliveryDriver, tenderId, deliveryWeek))
                .expectNext(Tuples.of(100, 50))
                .verifyComplete();
    }

    @Test
    void retrieveDeclaredAndUsedCapacity_handlesNoUsedCapacity() {
        String geoKey = "geo1";
        String unifiedDeliveryDriver = "driver1";
        String tenderId = "tender1";
        LocalDate deliveryWeek = LocalDate.now();

        when(paperDeliveryUsedCapacityDAO.get(unifiedDeliveryDriver, geoKey, deliveryWeek))
                .thenReturn(Mono.empty());
        when(paperDeliveryDriverCapacitiesDAO.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geoKey, deliveryWeek))
                .thenReturn(Mono.just(100));

        StepVerifier.create(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(geoKey, unifiedDeliveryDriver, tenderId, deliveryWeek))
                .expectNext(Tuples.of(100, 0))
                .verifyComplete();
    }

    @Test
    void retrieveDriversCapacityOnProvinceWithoutExclude() {
        LocalDate deliveryDate = LocalDate.now();
        String tenderId = "tender1";
        String province = "RM";
        PaperDeliveryDriverCapacity driverCapacity1 = new PaperDeliveryDriverCapacity();
        driverCapacity1.setUnifiedDeliveryDriver("driver1");
        driverCapacity1.setCapacity(30);
        driverCapacity1.setProducts(List.of("RS","AR"));
        PaperDeliveryDriverCapacity driverCapacity2 = new PaperDeliveryDriverCapacity();
        driverCapacity2.setUnifiedDeliveryDriver("driver2");
        driverCapacity2.setCapacity(30);
        driverCapacity2.setProducts(List.of("890"));
        PaperDeliveryDriverCapacity driverCapacity3 = new PaperDeliveryDriverCapacity();
        driverCapacity3.setUnifiedDeliveryDriver("driver3");
        driverCapacity3.setCapacity(30);
        driverCapacity3.setProducts(List.of("RS"));

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate.toString(), "EXCLUDE~" + province))
                .thenReturn(Mono.empty());
        when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(List.of(driverCapacity2, driverCapacity1, driverCapacity3)));

        StepVerifier.create(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province))
                .expectNextMatches(result -> result.getFirst().getCapacity() == 60 && result.getFirst().getUnifiedDeliveryDrivers().size() == 2)
                .verifyComplete();
    }

    @Test
    void retrieveDriversCapacityOnProvinceWithExclude() {
        LocalDate deliveryDate = LocalDate.now();
        String tenderId = "tender1";
        String province = "RM";
        PaperDeliveryDriverCapacity driverCapacity1 = new PaperDeliveryDriverCapacity();
        driverCapacity1.setUnifiedDeliveryDriver("driver1");
        driverCapacity1.setCapacity(30);
        driverCapacity1.setProducts(List.of("RS","AR"));
        PaperDeliveryDriverCapacity driverCapacity2 = new PaperDeliveryDriverCapacity();
        driverCapacity2.setUnifiedDeliveryDriver("driver2");
        driverCapacity2.setCapacity(30);
        driverCapacity2.setProducts(List.of("RS"));

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setSk("EXCLUDE~RS~" + province);
        paperDeliveryCounter.setNumberOfShipments(10);

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate.toString(), "EXCLUDE~" + province))
                .thenReturn(Mono.just(List.of(paperDeliveryCounter)));
        when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(List.of(driverCapacity2, driverCapacity1)));

        StepVerifier.create(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province))
                .expectNextMatches(result -> result.getFirst().getCapacity() == 50 && result.getFirst().getUnifiedDeliveryDrivers().size() == 2)
                .verifyComplete();
    }

    @Test
    void updateCounter(){
        List<IncrementUsedCapacityDto> incrementCapacities = List.of(
                new IncrementUsedCapacityDto("unifiedDeliveryDriver", "geoKey", 10, LocalDate.now(), 100),
                new IncrementUsedCapacityDto("unifiedDeliveryDriver2", "geoKey2", 5, LocalDate.now(), 50)
        );

        when(paperDeliveryUsedCapacityDAO.updateCounter(
                "unifiedDeliveryDriver", "geoKey", 10, LocalDate.now(), 100))
                .thenReturn(Mono.empty());

        when(paperDeliveryUsedCapacityDAO.updateCounter(
                "unifiedDeliveryDriver2", "geoKey2", 5, LocalDate.now(), 50))
                .thenReturn(Mono.empty());

        StepVerifier.create(deliveryDriverUtils.updateCounters(incrementCapacities))
                .verifyComplete();

        verify(paperDeliveryUsedCapacityDAO, times(2)).updateCounter(any(), any(), any(), any(), any());
    }

    @Test
    void insertInCache() {
        List<PaperChannelDeliveryDriverResponse> responses = List.of(new PaperChannelDeliveryDriverResponse("geoKey", "AR", "unifiedDeliveryDriver"));
        when(pnDelayerUtils.groupByGeoKeyAndProduct(any())).thenReturn(Map.of("geoKey~AR", "unifiedDeliveryDriver"));
        deliveryDriverUtils.insertInCache(responses);
        verify(cacheService, times(1)).addToCache("geoKey~AR", "unifiedDeliveryDriver");
    }

    @Test
    void retrieveFromCache() {
        when(cacheService.getFromCache("capProductTypeKey")).thenReturn(Optional.of("unifiedDeliveryDriver"));
        var result = deliveryDriverUtils.retrieveFromCache("capProductTypeKey");
        assertEquals("unifiedDeliveryDriver", result.orElse(null));
    }
}

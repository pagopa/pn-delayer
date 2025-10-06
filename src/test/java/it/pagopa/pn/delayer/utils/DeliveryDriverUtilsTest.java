package it.pagopa.pn.delayer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriver;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeliveryDriverUtilsTest {

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
        SdkBytes sdkBytesResponse = SdkBytes.fromUtf8String("{\"body\":[{\"unifiedDeliveryDriver\":\"driver1\"}]}");

        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(sdkBytesResponse).build());

        List<PaperChannelDeliveryDriver> result = deliveryDriverUtils.retrieveUnifiedDeliveryDriversFromPaperChannel(deliveryDriverRequests, tenderId);

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

        String skCounter = PaperDeliveryCounter.buildSkPrefix(PaperDeliveryCounter.SkPrefix.EXCLUDE, province);
        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate.toString(), skCounter, null))
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
        paperDeliveryCounter.setSk("EXCLUDE~" + province + "~RS");
        paperDeliveryCounter.setNumberOfShipments(10);

        String skCounter = PaperDeliveryCounter.buildSkPrefix(PaperDeliveryCounter.SkPrefix.EXCLUDE, province);
        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate.toString(), skCounter, null))
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
                new IncrementUsedCapacityDto("unifiedDeliveryDriver", "geoKey2", 5, LocalDate.now(), 100)
        );

        when(paperDeliveryUsedCapacityDAO.updateCounter(
                "unifiedDeliveryDriver", "geoKey", 10, LocalDate.now(), 100))
                .thenReturn(Mono.empty());

        when(paperDeliveryUsedCapacityDAO.updateCounter(
                "unifiedDeliveryDriver", "geoKey2", 5, LocalDate.now(), 100))
                .thenReturn(Mono.empty());

        StepVerifier.create(deliveryDriverUtils.updateCounters(incrementCapacities))
                .verifyComplete();

        verify(paperDeliveryUsedCapacityDAO, times(2)).updateCounter(any(), any(), any(), any(), any());
    }

    @Test
    void insertInCache() {
        List<PaperChannelDeliveryDriver> responses = List.of(new PaperChannelDeliveryDriver("geoKey", "AR", "unifiedDeliveryDriver"));
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

    @Test
    void assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(){

        List<PaperDelivery> deliveries1 = new ArrayList<>();
        deliveries1.add(createPaperDelivery("AR", "00178", "RM", "paId1", 0));
        List<PaperDelivery> deliveries2 = new ArrayList<>();
        deliveries2.add(createPaperDelivery("RS", "00179", "RM", "paId2", 1));

        when(cacheService.getFromCache("00178~AR")).thenReturn(Optional.of("driverX"));
        when(cacheService.getFromCache("00179~RS")).thenReturn(Optional.of("driverY"));
        Map<String, List<PaperDelivery>> grouped = Map.of(
                "00178~AR", deliveries1,
                "00179~RS", deliveries2
        );

        Map<Integer, List<String>> priorityMap = Map.of(
                1, List.of("PRODUCT_AR.ATTEMPT_0"),
                2, List.of("PRODUCT_RS.ATTEMPT_1")
        );

        List<PaperDelivery> result = deliveryDriverUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(
                grouped, "tenderTest", priorityMap
        );

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(d -> d.getUnifiedDeliveryDriver().equals("driverX") && d.getPriority() == 1));
        assertTrue(result.stream().anyMatch(d -> d.getUnifiedDeliveryDriver().equals("driverY") && d.getPriority() == 2));
    }

    @Test
    void enrichWithPriorityAndUnifiedDeliveryDriver(){
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 1));
        paperDeliveries.add(createPaperDelivery("RS","00179", "RM", "paId2", 0));
        paperDeliveries.add(createPaperDelivery("AR","00179", "RM", "paId2", 0));
        Map<Integer, List<String>> priorityMap = Map.of(
                1, List.of("PRODUCT_RS.ATTEMPT_0"),
                2, List.of("PRODUCT_AR.ATTEMPT_1","PRODUCT_890.ATTEMPT_1"),
                3, List.of("PRODUCT_AR.ATTEMPT_0","PRODUCT_890.ATTEMPT_0"));

        List<PaperDelivery> result = deliveryDriverUtils.enrichWithPriorityAndUnifiedDeliveryDriver(paperDeliveries, "driver2", "tenderId", priorityMap);

        assertEquals(3, result.size());
        assertEquals(2, result.get(0).getPriority());
        assertEquals("driver2", result.get(0).getUnifiedDeliveryDriver());
        assertEquals("tenderId", result.get(0).getTenderId());
        assertEquals(1, result.get(1).getPriority());
        assertEquals("driver2", result.get(1).getUnifiedDeliveryDriver());
        assertEquals("tenderId", result.get(1).getTenderId());
        assertEquals(3, result.get(2).getPriority());
        assertEquals("driver2", result.get(2).getUnifiedDeliveryDriver());
        assertEquals("tenderId", result.get(2).getTenderId());
    }

    private PaperChannelDeliveryDriver createPaperChannelDeliveryDriver(String geoKey, String product, String driver) {
        PaperChannelDeliveryDriver response = new PaperChannelDeliveryDriver();
        response.setGeoKey(geoKey);
        response.setProduct(product);
        response.setUnifiedDeliveryDriver(driver);
        return response;
    }

    private PaperDelivery createPaperDelivery(String productType, String cap, String province, String senderPaId, Integer attempt) {
        PaperDelivery delivery = new PaperDelivery();
        delivery.setCap(cap);
        delivery.setProvince(province);
        delivery.setProductType(productType);
        delivery.setUnifiedDeliveryDriver("driver1");
        delivery.setRequestId("requestId");
        delivery.setNotificationSentAt("2023-10-01T12:00:00Z");
        delivery.setSenderPaId(senderPaId);
        delivery.setDeliveryDate("2023-10-02");
        delivery.setPriority(1);
        delivery.setAttempt(attempt);
        delivery.setDeliveryDate("2023-10-02");
        delivery.setPk("2023-10-02~EVALUATE_RESIDUAL_CAPACITY");
        return delivery;
    }

}

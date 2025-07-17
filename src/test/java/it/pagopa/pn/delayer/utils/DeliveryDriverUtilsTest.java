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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

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
    void retrieveDriversCapacityOnProvince_handlesValidData() {
        LocalDate deliveryDate = LocalDate.now();
        String tenderId = "tender1";
        String province = "RM";
        List<PaperDeliveryDriverCapacity> driverCapacities = List.of(
                new PaperDeliveryDriverCapacity(),
                new PaperDeliveryDriverCapacity()
        );

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "EXCLUDE~" + province))
                .thenReturn(Mono.just(new PaperDeliveryCounter()));
        when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(driverCapacities));

        StepVerifier.create(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province))
                .expectNextMatches(result -> result.getCapacity() == 60 && result.getUnifiedDeliveryDrivers().size() == 2)
                .verifyComplete();
    }

    @Test
    void retrieveDriversCapacityOnProvince_handlesNoDriverCapacities() {
        LocalDate deliveryDate = LocalDate.now();
        String tenderId = "tender1";
        String province = "RM";

        when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "EXCLUDE~" + province))
                .thenReturn(Mono.just(new PaperDeliveryCounter()));
        when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(List.of()));

        StepVerifier.create(deliveryDriverUtils.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province))
                .expectNextMatches(result -> result.getCapacity() == -20 && result.getUnifiedDeliveryDrivers().isEmpty())
                .verifyComplete();
    }
}

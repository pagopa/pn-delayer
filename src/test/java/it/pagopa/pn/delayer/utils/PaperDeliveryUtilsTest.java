package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverRequest;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_SENDER_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryUtilsTest {


    private PaperDeliveryUtils paperDeliveryUtils;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        pnDelayerConfigs.setDeliveryDateDayOfWeek(1);
        pnDelayerConfigs.setDeliveryDateInterval(Duration.ofDays(7));
        paperDeliveryUtils = new PaperDeliveryUtils(pnDelayerConfigs);
    }

    @Test
    void filterAndPrepareDeliveries_returnsCorrectSize_whenDeliveriesFitCapacity() {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        paperDelivery1.setPriority(1);
        paperDelivery1.setRequestId("requestId1");
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery2.setPriority(1);
        paperDelivery2.setRequestId("requestId2");
        List<PaperDelivery> deliveries = List.of(paperDelivery1, paperDelivery2);
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(5, 2);
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();

        Integer result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, capacityTuple, deliveriesToSend, new ArrayList<>(), LocalDate.parse("2025-01-01"));

        assertEquals(2, result);
        assertEquals(2, deliveriesToSend.size());
    }

    @Test
    void filterAndPrepareDeliveries_returnsZero_whenNoRemainingCapacity() {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        List<PaperDelivery> deliveries = List.of(paperDelivery1, paperDelivery2);
        Tuple2<Integer, Integer> capacityTuple = Tuples.of(2, 2);
        List<PaperDelivery> deliveriesToSend = new ArrayList<>();

        Integer result = paperDeliveryUtils.filterAndPrepareDeliveries(deliveries, capacityTuple, deliveriesToSend, new ArrayList<>(), LocalDate.parse("2025-01-01"));

        assertEquals(0, result);
        assertTrue(deliveriesToSend.isEmpty());
    }

    @Test
    void groupDeliveryOnCapAndOrderOnCreatedAt_groupsAndSortsCorrectly() {
        PaperDelivery paperDelivery1 = new PaperDelivery();
        paperDelivery1.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery1.setSk("RM~2025-01-01t00:00:00Z~requestId1");
        paperDelivery1.setCap("12345");
        paperDelivery1.setCreatedAt(Instant.now().toString());
        PaperDelivery paperDelivery2 = new PaperDelivery();
        paperDelivery2.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery2.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery2.setCap("12345");
        paperDelivery2.setCreatedAt(Instant.now().toString());
        PaperDelivery paperDelivery3 = new PaperDelivery();
        paperDelivery3.setPk("2025-01-01~" + EVALUATE_SENDER_LIMIT);
        paperDelivery3.setSk("RM~2025-01-01t00:00:00Z~requestId2");
        paperDelivery3.setCap("67890");
        paperDelivery3.setCreatedAt(Instant.now().toString());
        List<PaperDelivery> deliveries = List.of(paperDelivery1, paperDelivery2, paperDelivery3);

        Map<String, List<PaperDelivery>> result = paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(deliveries);

        assertEquals(2, result.size());
        assertEquals(List.of(paperDelivery1, paperDelivery2), result.get("12345"));
        assertEquals(List.of(paperDelivery3), result.get("67890"));
    }

    @Test
    void calculateDeliveryWeek_returnsCorrectStartOfWeek() {
        Instant startExecutionBatch = Instant.parse("2023-10-04T10:00:00Z");

        LocalDate result = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);

        assertEquals(LocalDate.parse("2023-10-02"), result);
    }

    @Test
    void toNextWeek_correctlyUpdatesPkAndSk() {
        PaperDelivery paperDelivery = new PaperDelivery();
        paperDelivery.setProvince("RM");
        paperDelivery.setRequestId("requestId1");
        paperDelivery.setProductType("RS");
        paperDelivery.setPrepareRequestDate("2023-10-01");
        List<PaperDelivery> deliveries = List.of(paperDelivery);
        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");

        List<PaperDelivery> result = paperDeliveryUtils.toNextWeek(deliveries, deliveryWeek);

        assertEquals(1, result.size());
        assertEquals("2023-10-09~EVALUATE_SENDER_LIMIT", result.getFirst().getPk());
        assertEquals("RM~2023-10-01~requestId1", result.getFirst().getSk());
    }

    @Test
    void toNextWeek_handlesEmptyDeliveriesList() {
        List<PaperDelivery> deliveries = List.of();
        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");

        List<PaperDelivery> result = paperDeliveryUtils.toNextWeek(deliveries, deliveryWeek);

        assertTrue(result.isEmpty());
    }

    @Test
    void toNextWeek_usesNotificationSentAtForNonRSAndNonFirstAttempt() {
        PaperDelivery paperDelivery = new PaperDelivery();
        paperDelivery.setProvince("RM");
        paperDelivery.setRequestId("requestId2");
        paperDelivery.setProductType("Non-RS");
        paperDelivery.setAttempt(2);
        paperDelivery.setNotificationSentAt("2023-10-03");
        List<PaperDelivery> deliveries = List.of(paperDelivery);
        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");

        List<PaperDelivery> result = paperDeliveryUtils.toNextWeek(deliveries, deliveryWeek);

        assertEquals(1, result.size());
        assertEquals("2023-10-09~EVALUATE_SENDER_LIMIT", result.getFirst().getPk());
        assertEquals("RM~2023-10-03~requestId2", result.getFirst().getSk());
    }

    @Test
    void buildDriverCapacityEvaluation_createsCorrectPaperDeliveryObjects() {
        PaperDelivery paperDelivery = new PaperDelivery();
        paperDelivery.setProductType("RS");
        paperDelivery.setAttempt(1);
        paperDelivery.setDeliveryDate("2023-10-01");
        paperDelivery.setCap("12345");
        paperDelivery.setRecipientId("recipient1");
        paperDelivery.setIun("iun1");
        paperDelivery.setRequestId("requestId1");
        paperDelivery.setCreatedAt("2023-10-01T10:00:00Z");
        paperDelivery.setNotificationSentAt("2023-10-01T12:00:00Z");
        paperDelivery.setPrepareRequestDate("2023-10-01");
        paperDelivery.setSenderPaId("sender1");
        paperDelivery.setProvince("RM");

        List<PaperDelivery> deliveries = List.of(paperDelivery);
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("PRODUCT_RS.ATTEMPT_1"));
        String unifiedDeliveryDriver = "driver1";
        String tenderId = "tender1";

        List<PaperDelivery> result = paperDeliveryUtils.buildtoDriverCapacityEvaluation(deliveries, unifiedDeliveryDriver, tenderId, priorityMap);

        assertEquals(1, result.size());
        PaperDelivery resultDelivery = result.get(0);
        assertEquals("driver1", resultDelivery.getUnifiedDeliveryDriver());
        assertEquals("tender1", resultDelivery.getTenderId());
        assertEquals(1, resultDelivery.getPriority());
    }

    @Test
    void assignUnifiedDeliveryDriverAndBuildNewStepEntities_filtersAndBuildsCorrectly() {
        PaperChannelDeliveryDriverResponse response = new PaperChannelDeliveryDriverResponse();
        response.setGeoKey("RM");
        response.setProduct("RS");
        response.setUnifiedDeliveryDriver("driver1");

        PaperDelivery paperDelivery = new PaperDelivery();
        paperDelivery.setCap("12345");
        paperDelivery.setProductType("RS");
        paperDelivery.setAttempt(1);
        paperDelivery.setDeliveryDate("2023-10-01");
        paperDelivery.setRequestId("requestId1");

        List<PaperChannelDeliveryDriverResponse> responses = List.of(response);
        Map<String, List<PaperDelivery>> groupedByCapProductType = Map.of("RM~RS", List.of(paperDelivery));
        Map<Integer, List<String>> priorityMap = Map.of(1, List.of("PRODUCT_RS.ATTEMPT_1"));
        String tenderId = "tender1";

        List<PaperDelivery> result = paperDeliveryUtils.assignUnifiedDeliveryDriverAndBuildNewStepEntities(responses, groupedByCapProductType, tenderId, priorityMap);

        assertEquals(1, result.size());
        PaperDelivery resultDelivery = result.get(0);
        assertEquals("driver1", resultDelivery.getUnifiedDeliveryDriver());
        assertEquals("tender1", resultDelivery.getTenderId());
    }

    @Test
    void assignUnifiedDeliveryDriverAndBuildNewStepEntities_handlesEmptyGroupedByCapProductType() {
        List<PaperChannelDeliveryDriverResponse> responses = List.of();
        Map<String, List<PaperDelivery>> groupedByCapProductType = Map.of();
        Map<Integer, List<String>> priorityMap = Map.of();
        String tenderId = "tender1";

        List<PaperDelivery> result = paperDeliveryUtils.assignUnifiedDeliveryDriverAndBuildNewStepEntities(responses, groupedByCapProductType, tenderId, priorityMap);

        assertTrue(result.isEmpty());
    }

    @Test
    void groupByGeoKeyAndProduct_createsCorrectMapping() {
        PaperChannelDeliveryDriverResponse response1 = new PaperChannelDeliveryDriverResponse();
        response1.setGeoKey("RM");
        response1.setProduct("RS");
        response1.setUnifiedDeliveryDriver("driver1");

        PaperChannelDeliveryDriverResponse response2 = new PaperChannelDeliveryDriverResponse();
        response2.setGeoKey("MI");
        response2.setProduct("RS");
        response2.setUnifiedDeliveryDriver("driver2");

        List<PaperChannelDeliveryDriverResponse> responses = List.of(response1, response2);

        Map<String, String> result = paperDeliveryUtils.groupByGeoKeyAndProduct(responses);

        assertEquals(2, result.size());
        assertEquals("driver1", result.get("RM~RS"));
        assertEquals("driver2", result.get("MI~RS"));
    }

    @Test
    void constructPaperChannelTenderApiPayloadTest(){
        List<DeliveryDriverRequest> deliveryDriverRequests = new ArrayList<>();
        deliveryDriverRequests.add(new DeliveryDriverRequest("geoKey1", "product1"));
        String tenderId = "tender123";

        PaperChannelDeliveryDriverRequest response = paperDeliveryUtils.constructPaperChannelTenderApiPayload(deliveryDriverRequests, tenderId);

        assertEquals(tenderId, response.getTenderId());
        assertEquals("GET_UNIFIED_DELIVERY_DRIVERS", response.getOperation());
        assertEquals(deliveryDriverRequests, response.getRequests());
    }


}

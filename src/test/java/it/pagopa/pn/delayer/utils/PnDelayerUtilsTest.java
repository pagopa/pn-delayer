package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import it.pagopa.pn.delayer.model.SenderLimitJobProcessObjects;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.model.WorkflowStepEnum.EVALUATE_SENDER_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class PnDelayerUtilsTest {


    private PnDelayerUtils pnDelayerUtils;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs pnDelayerConfigs = new PnDelayerConfigs();
        pnDelayerConfigs.setDeliveryDateDayOfWeek(1);
        pnDelayerUtils = new PnDelayerUtils(pnDelayerConfigs);
    }

    @Test
    void testCalculateDeliveryWeek() {
        Instant startExecutionBatch = Instant.parse("2023-10-02T00:00:00Z"); // Monday
        LocalDate expectedDeliveryWeek = LocalDate.parse("2023-10-02");
        LocalDate actualDeliveryWeek = pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch);
        assertEquals(expectedDeliveryWeek, actualDeliveryWeek);
    }

    @Test
    void testGroupByCap() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();

        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId1", 0));

        Map<String, List<PaperDelivery>> grouped = pnDelayerUtils.groupByCap(paperDeliveries);
        assertEquals(2, grouped.size());
        assertEquals(3, grouped.get("00178").size());
        assertEquals(1, grouped.get("00179").size());
    }


    @Test
    void groupByPaIdProductTypeProvince() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();

        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00178", "NA", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId2", 0));


        Map<String, List<PaperDelivery>> grouped = pnDelayerUtils.groupByPaIdProductTypeProvince(paperDeliveries);
        assertEquals(4, grouped.size());
        assertEquals(2, grouped.get("paId1~AR~RM").size());
        assertEquals(1, grouped.get("paId1~890~RM").size());
        assertEquals(1, grouped.get("paId2~890~RM").size());
        assertEquals(1, grouped.get("paId1~890~NA").size());
    }


    @Test
    void groupByPaIdProductTypeProvinceAndCount() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();

        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00178", "NA", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId2", 0));


        Map<String, Long> grouped = pnDelayerUtils.groupByPaIdProductTypeProvinceAndCount(paperDeliveries);
        assertEquals(4, grouped.size());
        assertEquals(2, grouped.get("paId1~AR~RM"));
        assertEquals(1, grouped.get("paId1~890~RM"));
        assertEquals(1, grouped.get("paId2~890~RM"));
        assertEquals(1, grouped.get("paId1~890~NA"));
    }

    @Test
    void groupByGeoKeyAndProduct() {
        List<PaperChannelDeliveryDriverResponse> paperDeliveries = new ArrayList<>();

        paperDeliveries.add(createPaperChannelDeliveryDriverResponse("00178","AR", "driver1"));
        paperDeliveries.add(createPaperChannelDeliveryDriverResponse("00178", "890", "driver2"));
        paperDeliveries.add(createPaperChannelDeliveryDriverResponse("00179","890", "driver2"));


        Map<String, String> grouped = pnDelayerUtils.groupByGeoKeyAndProduct(paperDeliveries);
        assertEquals(3, grouped.size());
        assertEquals("driver1", grouped.get("00178~AR"));
        assertEquals("driver2", grouped.get("00178~890"));
        assertEquals("driver2", grouped.get("00179~890"));
    }

    @Test
    void groupByCapAndProductType() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();

        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00178", "NA", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("890","00179", "RM", "paId2", 0));

        Map<String, List<PaperDelivery>> grouped = pnDelayerUtils.groupByCapAndProductType(paperDeliveries);
        assertEquals(3, grouped.size());
        assertEquals(2, grouped.get("00178~AR").size());
        assertEquals(1, grouped.get("00178~890").size());
        assertEquals(2, grouped.get("00179~890").size());
    }

    @Test
    void mapItemForResidualCapacityStep() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00179", "RM", "paId2", 0));

        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");
        List<PaperDelivery> result = pnDelayerUtils.mapItemForResidualCapacityStep(paperDeliveries, deliveryWeek);

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(delivery -> delivery.getPk().equalsIgnoreCase("2023-10-02~" + WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY.name())
        && delivery.getSk().equalsIgnoreCase(String.join("~", delivery.getUnifiedDeliveryDriver(), delivery.getProvince(), "2023-10-01T12:00:00Z", delivery.getRequestId()))));
    }

    @Test
    void mapItemForEvaluateDriverCapacityStep() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00179", "RM", "paId2", 0));

        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");
        List<PaperDelivery> result = pnDelayerUtils.mapItemForEvaluateDriverCapacityStep(paperDeliveries, deliveryWeek);

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(delivery -> delivery.getPk().equalsIgnoreCase("2023-10-02~" + WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY.name())
                && delivery.getSk().equalsIgnoreCase(String.join("~", delivery.getUnifiedDeliveryDriver(), delivery.getProvince(), String.valueOf(delivery.getPriority()),  "2023-10-01T12:00:00Z", delivery.getRequestId()))));
    }

    @Test
    void mapItemForEvaluatePrintCapacityStep() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00179", "RM", "paId2", 0));

        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");
        List<PaperDelivery> result = pnDelayerUtils.mapItemForEvaluatePrintCapacityStep(paperDeliveries, deliveryWeek);

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(delivery -> delivery.getPk().equalsIgnoreCase("2023-10-02~" + WorkflowStepEnum.EVALUATE_PRINT_CAPACITY.name())
                && delivery.getSk().equalsIgnoreCase(String.join("~", String.valueOf(delivery.getPriority()), "2023-10-01T12:00:00Z", delivery.getRequestId()))));
    }

    @Test
    void mapItemForEvaluateSenderLimitOnNextWeek() {
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00179", "RM", "paId2", 0));

        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");
        List<PaperDelivery> result = pnDelayerUtils.mapItemForEvaluateSenderLimitOnNextWeek(paperDeliveries, deliveryWeek);

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(delivery -> delivery.getPk().equalsIgnoreCase("2023-10-09~" + EVALUATE_SENDER_LIMIT.name())
                && delivery.getSk().equalsIgnoreCase(String.join("~", delivery.getProvince(), "2023-10-01T12:00:00Z", delivery.getRequestId()))));
    }

    @Test
    void filterOnResidualDriverCapacity(){
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(createPaperDelivery("AR","00178", "RM", "paId1", 0));
        paperDeliveries.add(createPaperDelivery("AR","00179", "RM", "paId2", 0));

        LocalDate deliveryWeek = LocalDate.parse("2023-10-02");
        Integer result = pnDelayerUtils.filterOnResidualDriverCapacity(paperDeliveries, Tuples.of(10,5), new ArrayList<>(), new ArrayList<>(), deliveryWeek);

        assertEquals(2, result);
    }

    @Test
    void assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(){
        List<PaperChannelDeliveryDriverResponse> driverResponses = new ArrayList<>();
        driverResponses.add(createPaperChannelDeliveryDriverResponse("00178", "AR", "driverX"));
        driverResponses.add(createPaperChannelDeliveryDriverResponse("00179", "RS", "driverY"));

        List<PaperDelivery> deliveries1 = new ArrayList<>();
        deliveries1.add(createPaperDelivery("AR", "00178", "RM", "paId1", 0));
        List<PaperDelivery> deliveries2 = new ArrayList<>();
        deliveries2.add(createPaperDelivery("RS", "00179", "RM", "paId2", 1));

        Map<String, List<PaperDelivery>> grouped = Map.of(
                "00178~AR", deliveries1,
                "00179~RS", deliveries2
        );

        Map<Integer, List<String>> priorityMap = Map.of(
                1, List.of("PRODUCT_AR.ATTEMPT_0"),
                2, List.of("PRODUCT_RS.ATTEMPT_1")
        );

        List<PaperDelivery> result = pnDelayerUtils.assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(
                driverResponses, grouped, "tenderTest", priorityMap
        );

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(d -> d.getUnifiedDeliveryDriver().equals("driverX") && d.getPriority() == 1));
        assertTrue(result.stream().anyMatch(d -> d.getUnifiedDeliveryDriver().equals("driverY") && d.getPriority() == 2));
    }

    @Test
    void evaluateSenderLimitAndFilterDeliveries(){
        Map<String, Tuple2<Integer, Integer>> senderLimitMap = Map.of(
                "paId1~AR~RM", Tuples.of(3, 1),
                "paId2~890~RM", Tuples.of(2, 0)
        );

        List<PaperDelivery> deliveries1 = new ArrayList<>();
        deliveries1.add(createPaperDelivery("AR", "00178", "RM", "paId1", 0));
        deliveries1.add(createPaperDelivery("AR", "00178", "RM", "paId1", 0));
        deliveries1.add(createPaperDelivery("AR", "00178", "RM", "paId1", 0));

        List<PaperDelivery> deliveries2 = new ArrayList<>();
        deliveries2.add(createPaperDelivery("890", "00179", "RM", "paId2", 0));
        deliveries2.add(createPaperDelivery("890", "00179", "RM", "paId2", 0));

        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId = Map.of(
                "paId1~AR~RM", deliveries1,
                "paId2~890~RM", deliveries2
        );

        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();

        pnDelayerUtils.evaluateSenderLimitAndFilterDeliveries(senderLimitMap, deliveriesGroupedByProductTypePaId, senderLimitJobProcessObjects);

        assertEquals(2, senderLimitJobProcessObjects.getSendToDriverCapacityStep().stream()
                .filter(d -> d.getSenderPaId().equals("paId1")).count());
        assertEquals(1, senderLimitJobProcessObjects.getSendToResidualCapacityStep().stream()
                .filter(d -> d.getSenderPaId().equals("paId1")).count());

        assertEquals(2, senderLimitJobProcessObjects.getSendToDriverCapacityStep().stream()
                .filter(d -> d.getSenderPaId().equals("paId2")).count());
        assertEquals(0, senderLimitJobProcessObjects.getSendToResidualCapacityStep().stream()
                .filter(d -> d.getSenderPaId().equals("paId2")).count());
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

        List<PaperDelivery> result = pnDelayerUtils.enrichWithPriorityAndUnifiedDeliveryDriver(paperDeliveries, "driver2", "tenderId", priorityMap);

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

    @Test
    void excludeRsAndSecondAttempt() {
        PaperDelivery paperDelivery1 = createPaperDelivery("RS", "00178", "RM", "paId1", 0);
        PaperDelivery paperDelivery2 = createPaperDelivery("RS", "00179", "RM", "paId2", 1);
        PaperDelivery paperDelivery3 = createPaperDelivery("AR", "00180", "RM", "paId3", 0);
        PaperDelivery paperDelivery4 = createPaperDelivery("890", "00181", "RM", "paId4", 1);
        List<PaperDelivery> items = List.of(paperDelivery1, paperDelivery2, paperDelivery3, paperDelivery4);
        SenderLimitJobProcessObjects senderLimitJobProcessObjects = new SenderLimitJobProcessObjects();

        List<PaperDelivery> result = pnDelayerUtils.excludeRsAndSecondAttempt(items, senderLimitJobProcessObjects);

        assertEquals(1, result.size());
        assertEquals(3, senderLimitJobProcessObjects.getSendToDriverCapacityStep().size());
    }

    private PaperChannelDeliveryDriverResponse createPaperChannelDeliveryDriverResponse(String geoKey, String product, String driver) {
        PaperChannelDeliveryDriverResponse response = new PaperChannelDeliveryDriverResponse();
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

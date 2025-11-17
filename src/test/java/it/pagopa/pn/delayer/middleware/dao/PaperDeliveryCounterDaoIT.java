package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaperDeliveryCounterDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    PnDelayerConfigs pnDelayerConfigs;

    @Test
    void getPaperDeliveryCounter() {

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s("2025-04-07").build());
        itemMap.put("sk", AttributeValue.builder().s("EXCLUDE~RM").build());
        itemMap.put("numberOfShipments", AttributeValue.builder().n("5").build());

        Map<String, AttributeValue> itemMap2 = new HashMap<>();
        itemMap2.put("pk", AttributeValue.builder().s("2025-04-07").build());
        itemMap2.put("sk", AttributeValue.builder().s("EXCLUDE~NA").build());
        itemMap2.put("numberOfShipments", AttributeValue.builder().n("10").build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder()
                        .item(itemMap)
                        .tableName(pnDelayerConfigs.getDao().getPaperDeliveryCounterTableName())
                        .build())
                .join();

        dynamoDbAsyncClient.putItem(PutItemRequest.builder()
                        .item(itemMap2)
                        .tableName(pnDelayerConfigs.getDao().getPaperDeliveryCounterTableName())
                        .build())
                .join();

        List<PaperDeliveryCounter> result = paperDeliveryCounterDAO.getPaperDeliveryCounter("2025-04-07", "EXCLUDE~RM", null).block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getFirst().getNumberOfShipments());
        Assertions.assertEquals("2025-04-07", result.getFirst().getPk());
        Assertions.assertEquals("EXCLUDE~RM", result.getFirst().getSk());

        List<PaperDeliveryCounter> result2 = paperDeliveryCounterDAO.getPaperDeliveryCounter("2025-04-07", "EXCLUDE~NA", null).block();
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(10, result2.getFirst().getNumberOfShipments());
        Assertions.assertEquals("2025-04-07", result2.getFirst().getPk());
        Assertions.assertEquals("EXCLUDE~NA", result2.getFirst().getSk());

        List<PaperDeliveryCounter> result3 = paperDeliveryCounterDAO.getPaperDeliveryCounter("2025-04-07", "EXCLUDE~MI", null).block();
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(0, result3.size());

    }

    @Test
    void updatePrintCapacityCounterTest() {
        pnDelayerConfigs.setDelayerToPaperChannelDailyScheduleCron("0 5-21 ? * TUE-SUN *");
        LocalDate deliveryDate = LocalDate.parse("2025-04-07");

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, 3000, 35000).block();
        List<PaperDeliveryCounter> result = paperDeliveryCounterDAO.getPaperDeliveryCounter("PRINT", deliveryDate.toString(), null).block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3000, result.getFirst().getNumberOfShipments());
        Assertions.assertEquals(deliveryDate.toString(), result.getFirst().getSk());
        Assertions.assertEquals("PRINT", result.getFirst().getPk());
        Assertions.assertEquals(5000, result.getFirst().getDailyPrintCapacity());
        Assertions.assertEquals(35000, result.getFirst().getWeeklyPrintCapacity());
        Assertions.assertEquals(0, result.getFirst().getSentToNextWeek());
        Assertions.assertEquals(0, result.getFirst().getSentToPhaseTwo());
        Assertions.assertEquals(17, result.getFirst().getDailyExecutions());
        Assertions.assertEquals(17, result.getFirst().getMondayExecutions());
        Assertions.assertEquals(0, result.getFirst().getDailyExecutionCounter());


        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, 3000, 5000).block();
        List<PaperDeliveryCounter> result2 = paperDeliveryCounterDAO.getPaperDeliveryCounter("PRINT", deliveryDate.toString(), null).block();
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(6000, result2.getFirst().getNumberOfShipments());
        Assertions.assertEquals(deliveryDate.toString(), result2.getFirst().getSk());
        Assertions.assertEquals("PRINT", result2.getFirst().getPk());
        Assertions.assertEquals(5000, result.getFirst().getDailyPrintCapacity());
        Assertions.assertEquals(35000, result.getFirst().getWeeklyPrintCapacity());
        Assertions.assertEquals(0, result.getFirst().getSentToNextWeek());
        Assertions.assertEquals(0, result.getFirst().getSentToPhaseTwo());
        Assertions.assertEquals(17, result.getFirst().getDailyExecutions());
        Assertions.assertEquals(17, result.getFirst().getMondayExecutions());
        Assertions.assertEquals(0, result.getFirst().getDailyExecutionCounter());
    }
}

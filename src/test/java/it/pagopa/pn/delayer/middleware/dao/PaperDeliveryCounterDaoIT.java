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
        itemMap.put("deliveryDate", AttributeValue.builder().s("2025-04-07").build());
        itemMap.put("sk", AttributeValue.builder().s("EXCLUDE~RM").build());
        itemMap.put("counterValue", AttributeValue.builder().n("5").build());

        Map<String, AttributeValue> itemMap2 = new HashMap<>();
        itemMap2.put("deliveryDate", AttributeValue.builder().s("2025-04-07").build());
        itemMap2.put("sk", AttributeValue.builder().s("EXCLUDE~NA").build());
        itemMap2.put("counterValue", AttributeValue.builder().n("10").build());

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

        PaperDeliveryCounter result = paperDeliveryCounterDAO.getPaperDeliveryCounter(LocalDate.parse("2025-04-07"), "EXCLUDE~RM").block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getCounter());
        Assertions.assertEquals(LocalDate.parse("2025-04-07"), result.getDeliveryDate());
        Assertions.assertEquals("EXCLUDE~RM", result.getSk());

        PaperDeliveryCounter result2 = paperDeliveryCounterDAO.getPaperDeliveryCounter(LocalDate.parse("2025-04-07"), "EXCLUDE~NA").block();
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(10, result2.getCounter());
        Assertions.assertEquals(LocalDate.parse("2025-04-07"), result2.getDeliveryDate());
        Assertions.assertEquals("EXCLUDE~NA", result2.getSk());

        PaperDeliveryCounter result3 = paperDeliveryCounterDAO.getPaperDeliveryCounter(LocalDate.parse("2025-04-07"), "EXCLUDE~MI").block();
        Assertions.assertNull(result3);

    }

    @Test
    void updatePrintCapacityCounterTest() {
        LocalDate deliveryDate = LocalDate.parse("2025-04-07");

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, 3000, 5000, null).block();
        PaperDeliveryCounter result = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3000, result.getCounter());
        Assertions.assertEquals(deliveryDate, result.getDeliveryDate());
        Assertions.assertEquals("PRINT", result.getSk());
        Assertions.assertEquals(5000, result.getWeeklyPrintCapacity());
        Assertions.assertNull(result.getExcludedDeliveryCounter());


        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, 3000, 5000, null).block();
        PaperDeliveryCounter result2 = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(6000, result2.getCounter());
        Assertions.assertEquals(deliveryDate, result2.getDeliveryDate());
        Assertions.assertEquals("PRINT", result2.getSk());
        Assertions.assertEquals(5000, result2.getWeeklyPrintCapacity());
        Assertions.assertNull(result2.getExcludedDeliveryCounter());

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, null, 5000, 500).block();
        PaperDeliveryCounter result3 = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(6000, result3.getCounter());
        Assertions.assertEquals(deliveryDate, result3.getDeliveryDate());
        Assertions.assertEquals("PRINT", result3.getSk());
        Assertions.assertEquals(5000, result3.getWeeklyPrintCapacity());
        Assertions.assertEquals(500, result3.getExcludedDeliveryCounter());

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, null, 5000, 500).block();
        PaperDeliveryCounter result4 = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result4);
        Assertions.assertEquals(6000, result4.getCounter());
        Assertions.assertEquals(deliveryDate, result4.getDeliveryDate());
        Assertions.assertEquals("PRINT", result4.getSk());
        Assertions.assertEquals(5000, result4.getWeeklyPrintCapacity());
        Assertions.assertEquals(1000, result4.getExcludedDeliveryCounter());


    }
}

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

        List<PaperDeliveryCounter> result = paperDeliveryCounterDAO.getPaperDeliveryCounter(LocalDate.parse("2025-04-07"), "EXCLUDE~RM").block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getFirst().getCounter());
        Assertions.assertEquals(LocalDate.parse("2025-04-07"), result.getFirst().getDeliveryDate());
        Assertions.assertEquals("EXCLUDE~RM", result.getFirst().getSk());

        List<PaperDeliveryCounter> result2 = paperDeliveryCounterDAO.getPaperDeliveryCounter(LocalDate.parse("2025-04-07"), "EXCLUDE~NA").block();
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(10, result2.getFirst().getCounter());
        Assertions.assertEquals(LocalDate.parse("2025-04-07"), result2.getFirst().getDeliveryDate());
        Assertions.assertEquals("EXCLUDE~NA", result2.getFirst().getSk());

        List<PaperDeliveryCounter> result3 = paperDeliveryCounterDAO.getPaperDeliveryCounter(LocalDate.parse("2025-04-07"), "EXCLUDE~MI").block();
        Assertions.assertNull(result3);

    }

    @Test
    void updatePrintCapacityCounterTest() {
        LocalDate deliveryDate = LocalDate.parse("2025-04-07");

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, 3000, 5000, null).block();
        List<PaperDeliveryCounter> result = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3000, result.getFirst().getCounter());
        Assertions.assertEquals(deliveryDate, result.getFirst().getDeliveryDate());
        Assertions.assertEquals("PRINT", result.getFirst().getSk());
        Assertions.assertEquals(5000, result.getFirst().getWeeklyPrintCapacity());
        Assertions.assertNull(result.getFirst().getExcludedDeliveryCounter());


        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, 3000, 5000, null).block();
        List<PaperDeliveryCounter> result2 = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(6000, result2.getFirst().getCounter());
        Assertions.assertEquals(deliveryDate, result2.getFirst().getDeliveryDate());
        Assertions.assertEquals("PRINT", result2.getFirst().getSk());
        Assertions.assertEquals(5000, result2.getFirst().getWeeklyPrintCapacity());
        Assertions.assertNull(result2.getFirst().getExcludedDeliveryCounter());

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, null, 5000, 500).block();
        List<PaperDeliveryCounter> result3 = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(6000, result3.getFirst().getCounter());
        Assertions.assertEquals(deliveryDate, result3.getFirst().getDeliveryDate());
        Assertions.assertEquals("PRINT", result3.getFirst().getSk());
        Assertions.assertEquals(5000, result3.getFirst().getWeeklyPrintCapacity());
        Assertions.assertEquals(500, result3.getFirst().getExcludedDeliveryCounter());

        paperDeliveryCounterDAO.updatePrintCapacityCounter(deliveryDate, null, 5000, 500).block();
        List<PaperDeliveryCounter> result4 = paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, "PRINT").block();
        Assertions.assertNotNull(result4);
        Assertions.assertEquals(6000, result4.getFirst().getCounter());
        Assertions.assertEquals(deliveryDate, result4.getFirst().getDeliveryDate());
        Assertions.assertEquals("PRINT", result4.getFirst().getSk());
        Assertions.assertEquals(5000, result4.getFirst().getWeeklyPrintCapacity());
        Assertions.assertEquals(1000, result4.getFirst().getExcludedDeliveryCounter());


    }
}

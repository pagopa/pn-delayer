package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

class PaperDeliveryHighPriorityDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDao;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    PnDelayerConfigs pnDelayerConfigs;

    @Test
    void getPaperDeliveryHighPriority() {

        IntStream.range(0, 10).forEach(i -> {
            PaperDeliveryHighPriority paperDeliveryHighPriority = new PaperDeliveryHighPriority();
            paperDeliveryHighPriority.setDeliveryDriverIdGeoKey("deliveryDriverId1##geokey1");
            paperDeliveryHighPriority.setCreatedAt(Instant.parse("2025-04-07T00:00:00Z").plus(i, ChronoUnit.MINUTES));

            Map<String, AttributeValue> itemMap = new HashMap<>();
            itemMap.put("deliveryDriverIdGeokey", AttributeValue.builder().s(paperDeliveryHighPriority.getDeliveryDriverIdGeoKey()).build());
            itemMap.put("createdAt", AttributeValue.builder().s(paperDeliveryHighPriority.getCreatedAt().toString()).build());

            dynamoDbAsyncClient.putItem(PutItemRequest.builder()
                            .item(itemMap)
                            .tableName(pnDelayerConfigs.getDao().getPaperDeliveryHighPriorityTableName())
                            .build())
                    .join();
        });

        String deliveryDriverId = "deliveryDriverId1";
        String geokey = "geokey1";

        Page<PaperDeliveryHighPriority> result = paperDeliveryHighPriorityDao.getPaperDeliveryHighPriority(deliveryDriverId, geokey, null).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.items().size());
        System.out.println(result.items());
        Assertions.assertEquals("2025-04-07T00:00:00Z", result.items().get(0).getCreatedAt().toString());

        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("deliveryDriverIdGeokey", AttributeValue.builder().s(deliveryDriverId + "##" + geokey).build());
        lastEvaluatedKey.put("createdAt", AttributeValue.builder().s("2025-04-07T00:04:00Z").build());

        Page<PaperDeliveryHighPriority> resultWithLastEvaluated = paperDeliveryHighPriorityDao.getPaperDeliveryHighPriority(deliveryDriverId, geokey, lastEvaluatedKey).block();
        System.out.println(resultWithLastEvaluated.items());
        Assertions.assertNotNull(resultWithLastEvaluated);
        Assertions.assertEquals(5, resultWithLastEvaluated.items().size());
        Assertions.assertEquals("2025-04-07T00:05:00Z", resultWithLastEvaluated.items().get(0).getCreatedAt().toString());
    }

    @Test
    void transactionWriteItemOk() {
        List<PaperDeliveryHighPriority> highPrioritiesList = IntStream.range(0, 3)
                .mapToObj(i -> {
                    PaperDeliveryHighPriority paperDeliveryHighPriority = new PaperDeliveryHighPriority();
                    paperDeliveryHighPriority.setDeliveryDriverIdGeoKey("deliveryDriverId##geokey");
                    paperDeliveryHighPriority.setCreatedAt(Instant.parse("2025-04-07T00:00:00Z").plus(i, ChronoUnit.HOURS));
                    Map<String, AttributeValue> itemMap = new HashMap<>();
                    itemMap.put("deliveryDriverIdGeokey", AttributeValue.builder().s(paperDeliveryHighPriority.getDeliveryDriverIdGeoKey()).build());
                    itemMap.put("createdAt", AttributeValue.builder().s(paperDeliveryHighPriority.getCreatedAt().toString()).build());
                    dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryHighPriorityTableName()).build()).join();
                    return paperDeliveryHighPriority;
                })
                .toList();

        List<PaperDeliveryReadyToSend> readyToSendList = IntStream.range(0, 3)
                .mapToObj(i -> {
                    PaperDeliveryReadyToSend paperDeliveryReadyToSend = new PaperDeliveryReadyToSend();
                    paperDeliveryReadyToSend.setDeliveryDate(Instant.parse("2025-04-14T00:00:00Z"));
                    paperDeliveryReadyToSend.setRequestId(UUID.randomUUID().toString());
                    return paperDeliveryReadyToSend;
                })
                .toList();

        paperDeliveryHighPriorityDao.executeTransaction(highPrioritiesList, readyToSendList).block();

        Page<PaperDeliveryHighPriority> highPriorityResponse = paperDeliveryHighPriorityDao.getPaperDeliveryHighPriority("deliveryDriverId","geoKey", new HashMap<>()).block();

        assert highPriorityResponse != null;
        Assertions.assertTrue(highPriorityResponse.items().isEmpty());

        List<Map<String, AttributeValue>> readyToSendResponses = dynamoDbAsyncClient.query(queryRequest -> queryRequest
                .tableName(pnDelayerConfigs.getDao().getPaperDeliveryReadyToSendTableName())
                .keyConditionExpression("deliveryDate = :deliveryDate")
                .expressionAttributeValues(Map.of(
                        ":deliveryDate", AttributeValue.builder().s("2025-04-14T00:00:00Z").build()
                ))
        ).join().items();
        Assertions.assertEquals(3, readyToSendResponses.size());
    }
}

package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

class PaperDeliveryDriverCapacitiesDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDriverCapacitiesDAO paperDeliveryDriverCapacitiesDao;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    PnDelayerConfigs pnDelayerConfigs;

    @Test
    void getPaperDeliveryDriverCapacitiesCloseIntervalSelected() {

        String tenderId = "tenderId";
        String deliveryDriverId = "deliveryDriverId1";
        String geokey = "geokey1";
        String capacity = "10";
        Instant now = Instant.now();
        Instant past = now.minus(10, ChronoUnit.DAYS);
        Instant future = now.plus(10, ChronoUnit.DAYS);

        PaperDeliveryDriverCapacity capacities  = new PaperDeliveryDriverCapacity();
        capacities.setPk(String.join("##", tenderId, deliveryDriverId, geokey));
        capacities.setActivationDateFrom(past);
        capacities.setActivationDateTo(future);

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(capacities.getPk()).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(capacities.getActivationDateFrom().toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(capacities.getActivationDateTo().toString()).build());
        itemMap.put("capacity", AttributeValue.builder().s(capacity).build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build());

        itemMap.put("activationDateFrom", AttributeValue.builder().s("2025-01-01T00:00:00Z").build());
        itemMap.remove("activationDateTo");
        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build());

        itemMap.put("activationDateFrom", AttributeValue.builder().s(now.minus(60, ChronoUnit.DAYS).toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(now.minus(30, ChronoUnit.DAYS).toString()).build());
        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build());

        PaperDeliveryDriverCapacity result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geokey, now).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(capacities.getPk(), result.getPk());
        Assertions.assertEquals(capacities.getActivationDateFrom(), result.getActivationDateFrom());
        Assertions.assertEquals(capacities.getActivationDateTo(), result.getActivationDateTo());
    }

    @Test
    void getPaperDeliveryDriverCapacitiesOpenIntervalSelected() {
        String tenderId = "tenderId";
        String deliveryDriverId = "deliveryDriverId2";
        String geokey = "geokey2";
        String capacity = "10";
        Instant now = Instant.now();
        String pk = String.join("##", tenderId, deliveryDriverId, geokey);

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(pk).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(now.minus(90, ChronoUnit.DAYS).toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(now.minus(60, ChronoUnit.DAYS).toString()).build());
        itemMap.put("capacity", AttributeValue.builder().s(capacity).build());

        Mono.fromFuture(dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build())).block();

        itemMap.put("activationDateFrom", AttributeValue.builder().s("2025-01-01T00:00:00Z").build());
        itemMap.remove("activationDateTo");
        Mono.fromFuture(dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build())).block();

        itemMap.put("activationDateFrom", AttributeValue.builder().s(now.minus(60, ChronoUnit.DAYS).toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(now.minus(30, ChronoUnit.DAYS).toString()).build());
        Mono.fromFuture(dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build())).block();

        PaperDeliveryDriverCapacity result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geokey, now).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(pk, result.getPk());
        Assertions.assertEquals(Instant.parse("2025-01-01T00:00:00Z"), result.getActivationDateFrom());
        Assertions.assertNull(result.getActivationDateTo());
    }
}

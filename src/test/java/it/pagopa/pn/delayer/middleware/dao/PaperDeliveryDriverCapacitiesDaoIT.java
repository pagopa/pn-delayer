package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
        String unifiedDeliveryDriver = "unifiedDeliveryDriver1";
        String geokey = "geokey1";
        Instant now = Instant.now();
        LocalDate date = now.atZone(ZoneOffset.UTC).toLocalDate();
        Instant past = now.minus(10, ChronoUnit.DAYS);
        Instant future = now.plus(10, ChronoUnit.DAYS);

        PaperDeliveryDriverCapacity capacities  = new PaperDeliveryDriverCapacity();
        capacities.setPk(String.join("~", tenderId, unifiedDeliveryDriver, geokey));
        capacities.setActivationDateFrom(past);
        capacities.setActivationDateTo(future);

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(capacities.getPk()).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(capacities.getActivationDateFrom().toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(capacities.getActivationDateTo().toString()).build());
        itemMap.put("capacity", AttributeValue.builder().s("10").build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build()).join();

        itemMap.put("activationDateFrom", AttributeValue.builder().s("2025-01-01T00:00:00Z").build());
        itemMap.remove("activationDateTo");
        itemMap.put("capacity", AttributeValue.builder().s("30").build());
        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build()).join();

        itemMap.put("activationDateFrom", AttributeValue.builder().s(now.minus(60, ChronoUnit.DAYS).toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(now.minus(30, ChronoUnit.DAYS).toString()).build());
        itemMap.put("capacity", AttributeValue.builder().s("60").build());
        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build()).join();

        Integer result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geokey, date).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(10, result);
    }

    @Test
    void getPaperDeliveryDriverCapacitiesOpenIntervalSelected() {
        String tenderId = "tenderId";
        String unifiedDeliveryDriver = "unifiedDeliveryDriver2";
        String geokey = "geokey2";
        Instant now = Instant.now();
        LocalDate date = now.atZone(ZoneOffset.UTC).toLocalDate();
        String pk = String.join("~", tenderId, unifiedDeliveryDriver, geokey);

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(pk).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(now.minus(90, ChronoUnit.DAYS).toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(now.minus(60, ChronoUnit.DAYS).toString()).build());
        itemMap.put("capacity", AttributeValue.builder().s("10").build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build()).join();

        itemMap.put("activationDateFrom", AttributeValue.builder().s("2025-01-01T00:00:00Z").build());
        itemMap.remove("activationDateTo");
        itemMap.put("capacity", AttributeValue.builder().s("30").build());
        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build()).join();

        itemMap.put("activationDateFrom", AttributeValue.builder().s(now.minus(60, ChronoUnit.DAYS).toString()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(now.minus(30, ChronoUnit.DAYS).toString()).build());
        itemMap.put("capacity", AttributeValue.builder().s("20").build());
        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName()).build()).join();

        Integer result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, unifiedDeliveryDriver, geokey, date).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(30, result);
    }
}

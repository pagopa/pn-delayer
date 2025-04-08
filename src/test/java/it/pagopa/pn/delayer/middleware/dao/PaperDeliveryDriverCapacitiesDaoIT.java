package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.middleware.dao.dynamo.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class PaperDeliveryDriverCapacitiesDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDriverCapacitiesDAO paperDeliveryDriverCapacitiesDao;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Test
    void getPaperDeliveryDriverCapacities() {

        String tenderId = "tenderId";
        String deliveryDriverId = "deliveryDriverId";
        String geokey = "geokey";
        Instant now = Instant.now();
        Instant past = now.minusSeconds(6000);
        Instant future = now.plusSeconds(6000);

        PaperDeliveryDriverCapacities capacities  = new PaperDeliveryDriverCapacities();
        capacities.setPk(String.join("##", tenderId, deliveryDriverId, geokey));
        capacities.setActivationDateFrom(past.toString());
        capacities.setActivationDateTo(future.toString());

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(capacities.getPk()).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(capacities.getActivationDateFrom()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(capacities.getActivationDateTo()).build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName("pn-PaperDeliveryDriverCapacities").build());
        PaperDeliveryDriverCapacities result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geokey).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(capacities.getPk(), result.getPk());
        Assertions.assertEquals(capacities.getActivationDateFrom(), result.getActivationDateFrom());
    }

    @Test
    void getPaperDeliveryDriverCapacities_afterRange() {
        String tenderId = "tenderId";
        String deliveryDriverId = "deliveryDriverId";
        String geokey = "geokey";
        Instant now = Instant.now();
        Instant past = now.minusSeconds(12000);
        Instant beforePast = now.minusSeconds(6000);

        PaperDeliveryDriverCapacities capacities = new PaperDeliveryDriverCapacities();
        capacities.setPk(String.join("##", tenderId, deliveryDriverId, geokey));
        capacities.setActivationDateFrom(past.toString());
        capacities.setActivationDateTo(beforePast.toString());

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(capacities.getPk()).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(capacities.getActivationDateFrom()).build());
        itemMap.put("activationDateTo", AttributeValue.builder().s(capacities.getActivationDateTo()).build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName("pn-PaperDeliveryDriverCapacities").build()).join();
        PaperDeliveryDriverCapacities result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geokey).block();

        Assertions.assertNull(result);
    }

    @Test
    void getPaperDeliveryDriverCapacities_openRange() {
        String tenderId = "tenderId";
        String deliveryDriverId = "deliveryDriverId";
        String geokey = "geokey";
        Instant now = Instant.now();
        Instant past = now.minusSeconds(12000);

        PaperDeliveryDriverCapacities capacities = new PaperDeliveryDriverCapacities();
        capacities.setPk(String.join("##", tenderId, deliveryDriverId, geokey));
        capacities.setActivationDateFrom(past.toString());

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("pk", AttributeValue.builder().s(capacities.getPk()).build());
        itemMap.put("activationDateFrom", AttributeValue.builder().s(capacities.getActivationDateFrom()).build());

        dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName("pn-PaperDeliveryDriverCapacities").build()).join();
        PaperDeliveryDriverCapacities result = paperDeliveryDriverCapacitiesDao.getPaperDeliveryDriverCapacities(tenderId, deliveryDriverId, geokey).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(capacities.getPk(), result.getPk());
        Assertions.assertEquals(capacities.getActivationDateFrom(), result.getActivationDateFrom());
        Assertions.assertNull(result.getActivationDateTo());
    }
}

package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.LocalDate;
import java.util.Map;

class PaperDelivertPrintCapacityDaoIT extends BaseTest.WithLocalStack  {

    @Autowired
    PaperDeliveryPrintCapacityDAO paperDeliveryPrintCapacityDAO;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    PnDelayerConfigs pnDelayerConfigs;

    @Test
    void retrieveActualPrintCapacityTest() {
        LocalDate startDate = LocalDate.parse("2025-01-01");
        String pk = "PRINT";
        String printCapacity = "1000";

        dynamoDbAsyncClient.putItem(PutItemRequest.builder()
                .tableName(pnDelayerConfigs.getDao().getPaperDeliveryPrintCapacityTableName())
                .item(Map.of(PaperDeliveryPrintCapacity.COL_PK, AttributeValue.builder().s(pk).build(),
                        PaperDeliveryPrintCapacity.COL_START_DATE, AttributeValue.builder().s(startDate.toString()).build(),
                        PaperDeliveryPrintCapacity.COL_PRINT_CAPACITY, AttributeValue.builder().s(printCapacity).build()))
                .build())
                .join();

        Integer result = paperDeliveryPrintCapacityDAO.retrieveActualPrintCapacity(startDate).block();
        Assertions.assertEquals(1000, result);
    }

    @Test
    void retrieveActualPrintCapacityTestNotFound() {
        StepVerifier.create(paperDeliveryPrintCapacityDAO.retrieveActualPrintCapacity(LocalDate.parse("2025-01-01")))
                .expectError(PnInternalException.class)
                .verify();
    }
}

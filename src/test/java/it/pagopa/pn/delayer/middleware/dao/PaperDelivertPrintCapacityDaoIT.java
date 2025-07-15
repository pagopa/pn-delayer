package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacity;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacityCounter;
import it.pagopa.pn.delayer.model.PrintCapacityEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.model.PrintCapacityEnum.DAILY;
import static it.pagopa.pn.delayer.model.PrintCapacityEnum.WEEKLY;

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

    @Test
    void updatePrintCapacityCounterAndGet(){
        paperDeliveryPrintCapacityDAO.updatePrintCapacity(DAILY, LocalDate.parse("2025-02-05"), 10, 1000).block();
        paperDeliveryPrintCapacityDAO.updatePrintCapacity(WEEKLY, LocalDate.parse("2025-02-03"), 10, 7000).block();

        List<PaperDeliveryPrintCapacityCounter> result1 = paperDeliveryPrintCapacityDAO.retrievePrintCapacityCounters(LocalDate.parse("2025-02-03"), LocalDate.parse("2025-02-05")).block();
        Assertions.assertEquals(2, result1.size());
        Assertions.assertEquals("2025-02-03", result1.getFirst().getPk().toString());
        Assertions.assertEquals(10, result1.getFirst().getUsedPrintCapacity());
        Assertions.assertEquals(7000, result1.getFirst().getPrintCapacity());
        Assertions.assertEquals(WEEKLY.name(), result1.getFirst().getSk());
        Assertions.assertEquals("2025-02-05", result1.getLast().getPk().toString());
        Assertions.assertEquals(10, result1.getLast().getUsedPrintCapacity());
        Assertions.assertEquals(1000, result1.getLast().getPrintCapacity());
        Assertions.assertEquals(DAILY.name(), result1.getLast().getSk());

        paperDeliveryPrintCapacityDAO.updatePrintCapacity(DAILY, LocalDate.parse("2025-02-05"), 10, 1000).block();
        paperDeliveryPrintCapacityDAO.updatePrintCapacity(WEEKLY, LocalDate.parse("2025-02-03"), 10, 7000).block();

        List<PaperDeliveryPrintCapacityCounter> result2 = paperDeliveryPrintCapacityDAO.retrievePrintCapacityCounters(LocalDate.parse("2025-02-03"), LocalDate.parse("2025-02-05")).block();
        Assertions.assertEquals(2, result2.size());
        Assertions.assertEquals("2025-02-03", result2.getFirst().getPk().toString());
        Assertions.assertEquals(20, result2.getFirst().getUsedPrintCapacity());
        Assertions.assertEquals(7000, result2.getFirst().getPrintCapacity());
        Assertions.assertEquals(WEEKLY.name(), result2.getFirst().getSk());
        Assertions.assertEquals("2025-02-05", result2.getLast().getPk().toString());
        Assertions.assertEquals(20, result2.getLast().getUsedPrintCapacity());
        Assertions.assertEquals(1000, result2.getLast().getPrintCapacity());
        Assertions.assertEquals(DAILY.name(), result2.getLast().getSk());
    }

}

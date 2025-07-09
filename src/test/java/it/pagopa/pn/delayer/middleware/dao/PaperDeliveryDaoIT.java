package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
public class PaperDeliveryDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDAO paperDeliveryDAO;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    PnDelayerConfigs pnDelayerConfigs;

    @Test
    void insertAndRetrievePaperDeliveriesTest() {
        List<PaperDelivery> paperDeliveryList = new ArrayList<>();
        IntStream.range(0, 9).forEach(i -> {
            Instant now = Instant.parse("2025-04-09T00:00:00Z").plus(i, ChronoUnit.MINUTES);
            PaperDelivery paperDelivery = new PaperDelivery();
            paperDelivery.setPk("2025-04-07~" + WorkflowStepEnum.EVALUATE_SENDER_LIMIT);
            paperDelivery.setSk("RM~"+now+"~" + i);
            paperDeliveryList.add(paperDelivery);
        });

        Instant now = Instant.parse("2025-04-09T00:00:00Z");
        PaperDelivery paperDeliveryWithOtherProvince = new PaperDelivery();
        paperDeliveryWithOtherProvince.setPk("2025-04-07~" + WorkflowStepEnum.EVALUATE_SENDER_LIMIT);
        paperDeliveryWithOtherProvince.setSk("NA~"+now+"~TestProvince");
        paperDeliveryList.add(paperDeliveryWithOtherProvince);

        PaperDelivery paperDeliveryWithOtherStep = new PaperDelivery();
        paperDeliveryWithOtherStep.setPk("2025-04-07~" + WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY);
        paperDeliveryWithOtherStep.setSk("RM~"+now+"~TestStep");
        paperDeliveryList.add(paperDeliveryWithOtherStep);

        paperDeliveryDAO.insertPaperDeliveries(paperDeliveryList).block();

        Page<PaperDelivery> result = paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, "2025-04-07", "RM", null).block();
        log.info("result: {}", result);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.items().size());
        Assertions.assertTrue(result.items().stream().allMatch(paperDelivery -> paperDelivery.getPk().equalsIgnoreCase("2025-04-07~" + WorkflowStepEnum.EVALUATE_SENDER_LIMIT)
         && paperDelivery.getSk().startsWith("RM~")));
        Assertions.assertNotNull(result.lastEvaluatedKey());

        Page<PaperDelivery> resultWithLastEvaluated = paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, "2025-04-07", "RM", result.lastEvaluatedKey()).block();
        log.info("resultWithLastEvaluated: {}", resultWithLastEvaluated);
        Assertions.assertNotNull(resultWithLastEvaluated);
        Assertions.assertEquals(4, resultWithLastEvaluated.items().size());
        Assertions.assertTrue(resultWithLastEvaluated.items().stream().allMatch(paperDelivery -> paperDelivery.getPk().equalsIgnoreCase("2025-04-07~" + WorkflowStepEnum.EVALUATE_SENDER_LIMIT)
                && paperDelivery.getSk().startsWith("RM~")));
        Assertions.assertNull(resultWithLastEvaluated.lastEvaluatedKey());

        Page<PaperDelivery> resultForDriveCapacityStep = paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, "2025-04-07", "RM", null).block();
        log.info("resultForDriveCapacityStep: {}", resultForDriveCapacityStep);
        Assertions.assertNotNull(resultForDriveCapacityStep);
        Assertions.assertEquals(1, resultForDriveCapacityStep.items().size());
        Assertions.assertTrue(resultForDriveCapacityStep.items().stream().allMatch(paperDelivery -> paperDelivery.getPk().equalsIgnoreCase("2025-04-07~" + WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY)
                && paperDelivery.getSk().startsWith("RM~")));
        Assertions.assertNull(resultForDriveCapacityStep.lastEvaluatedKey());

        Page<PaperDelivery> resultForPrintCapacityStep = paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_PRINT_CAPACITY, "2025-04-07", "RM", null).block();
        log.info("resultForPrintCapacityStep: {}", resultForPrintCapacityStep);
        Assertions.assertNotNull(resultForPrintCapacityStep);
        Assertions.assertEquals(0, resultForPrintCapacityStep.items().size());

        Page<PaperDelivery> resultForOtherProvince = paperDeliveryDAO.retrievePaperDeliveries(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, "2025-04-07", "NA", null).block();
        log.info("resultForOtherProvince: {}", resultForOtherProvince);
        Assertions.assertNotNull(resultForOtherProvince);
        Assertions.assertEquals(1, resultForOtherProvince.items().size());
        Assertions.assertTrue(resultForOtherProvince.items().stream().allMatch(paperDelivery -> paperDelivery.getPk().equalsIgnoreCase("2025-04-07~" + WorkflowStepEnum.EVALUATE_SENDER_LIMIT)
                && paperDelivery.getSk().startsWith("NA~")));
        Assertions.assertNull(resultForOtherProvince.lastEvaluatedKey());

    }
}

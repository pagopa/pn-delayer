package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.IncrementUsedCapacityDto;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PaperDeliveryUtilsTest {

    @InjectMocks
    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private PaperDeliveryDAO paperDeliveryDAO;

    @ParameterizedTest
    @CsvSource({
            "10, 5, 5",
            "0, 0, 0",
            "15, 20, 0"
    })
    void evaluateCapacitiesAndProcessDeliveries_handlesResidualCapacityCorrectly(int declaredCapacity, int usedCapacity, int expectedResidualCapacity) {
        Tuple2<Integer, Integer> provinceCapacities = Tuples.of(declaredCapacity, usedCapacity);
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        LocalDate deliveryWeek = LocalDate.now();
        String tenderId = "tender1";

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(provinceCapacities, workflowStepEnum, unifiedDeliveryDriver, province, lastEvaluatedKey, deliveryWeek, tenderId))
                .verifyComplete();
    }


    @Test
    void retrievePaperDeliveries_handlesNoRecordsFound() {
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        LocalDate deliveryWeek = LocalDate.now();
        String sortKeyPrefix = "driver1~RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        int queryLimit = 10;

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(List.of())));

        StepVerifier.create(paperDeliveryUtils.retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, queryLimit))
                .verifyComplete();
    }

    @Test
    void retrievePaperDeliveries_handlesRecordsFound() {
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        LocalDate deliveryWeek = LocalDate.now();
        String sortKeyPrefix = "driver1~RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        int queryLimit = 10;

        PaperDelivery paperDelivery = new PaperDelivery();
        List<PaperDelivery> deliveries = new ArrayList<>();
        deliveries.add(paperDelivery);

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(deliveries)));

        StepVerifier.create(paperDeliveryUtils.retrievePaperDeliveries(workflowStepEnum, deliveryWeek, sortKeyPrefix, lastEvaluatedKey, queryLimit))
                .expectNextMatches(page -> !page.items().isEmpty() && page.items().get(0).getUnifiedDeliveryDriver().equals("delivery1"))
                .verifyComplete();
    }

    @Test
    void insertPaperDeliveries(){
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(new PaperDelivery());
        LocalDate deliveryWeek = LocalDate.now();

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.insertPaperDeliveries(new SenderLimitJobPaperDeliveries(), deliveryWeek))
                .expectNext(paperDeliveries)
                .verifyComplete();
    }
}

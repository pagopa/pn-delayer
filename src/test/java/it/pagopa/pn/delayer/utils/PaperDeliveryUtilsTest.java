package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryPrintCapacityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaperDeliveryUtilsTest {

    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private DeliveryDriverUtils deliveryDriverUtils;

    @Mock
    private PaperDeliveryDAO paperDeliveryDAO;

    @Mock
    private PaperDeliveryPrintCapacityDAO paperDeliveryPrintCapacityDAO;

    @Mock
    private PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    @BeforeEach
    void setUp() {
        PnDelayerConfigs config = new PnDelayerConfigs();
        PnDelayerConfigs.Dao daoConfig = new PnDelayerConfigs.Dao();
        daoConfig.setPaperDeliveryQueryLimit(10);
        config.setDao(daoConfig);
        paperDeliveryUtils = new PaperDeliveryUtils(paperDeliveryDAO, config, new PnDelayerUtils(config), deliveryDriverUtils, paperDeliveryCounterDAO, paperDeliveryPrintCapacityDAO);
    }


    void evaluateCapacitiesAndProcessDeliveries() {
        Tuple2<Integer, Integer> provinceCapacities = Tuples.of(10,5);
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        LocalDate deliveryWeek = LocalDate.now();
        String tenderId = "tender1";
        when(deliveryDriverUtils.updateCounters(anyList())).thenReturn(Mono.empty());
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(anyString(), anyString(), anyString(), any()))
                .thenReturn(Mono.just(provinceCapacities));

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(List.of(createPaperDelivery("AR", "00179", province, "senderPaId1", 1),
                        createPaperDelivery("AR", "00178", province, "senderPaId2", 1)))));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(workflowStepEnum, unifiedDeliveryDriver, province, deliveryWeek, tenderId))
                .verifyComplete();
    }

    void evaluateCapacitiesAndProcessDeliveriesNoCapacityOnProvince() {
        Tuple2<Integer, Integer> provinceCapacities = Tuples.of(10,10);
        WorkflowStepEnum workflowStepEnum = WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY;
        String unifiedDeliveryDriver = "driver1";
        String province = "RM";
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        LocalDate deliveryWeek = LocalDate.now();
        String tenderId = "tender1";
        when(deliveryDriverUtils.updateCounters(anyList())).thenReturn(Mono.empty());
        when(deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(anyString(), anyString(), anyString(), any()))
                .thenReturn(Mono.just(provinceCapacities));

        when(paperDeliveryDAO.retrievePaperDeliveries(any(), any(), anyString(), any(), anyInt()))
                .thenReturn(Mono.just(Page.create(List.of(createPaperDelivery("AR", "00179", province, "senderPaId1", 1),
                        createPaperDelivery("AR", "00178", province, "senderPaId2", 1)))));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        StepVerifier.create(paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(workflowStepEnum, unifiedDeliveryDriver, province, deliveryWeek, tenderId))
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
    void insertPaperDeliveries(){
        List<PaperDelivery> paperDeliveries = new ArrayList<>();
        paperDeliveries.add(new PaperDelivery());
        LocalDate deliveryWeek = LocalDate.now();

        SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries = new SenderLimitJobPaperDeliveries();
        senderLimitJobPaperDeliveries.setSendToDriverCapacityStep(List.of(createPaperDelivery("AR", "00100", "RM", "paId", 1)));
        senderLimitJobPaperDeliveries.setSendToResidualCapacityStep(List.of(createPaperDelivery("AR", "00100", "RM", "paId", 0)));

        when(paperDeliveryDAO.insertPaperDeliveries(anyList()))
                .thenReturn(Mono.empty());

        paperDeliveryUtils.insertPaperDeliveries(new SenderLimitJobPaperDeliveries(), deliveryWeek).block();

        verify(paperDeliveryDAO,times(2)).insertPaperDeliveries(anyList());
    }

    private PaperDelivery createPaperDelivery(String productType, String cap, String province, String senderPaId, Integer attempt) {
        PaperDelivery delivery = new PaperDelivery();
        delivery.setCap(cap);
        delivery.setProvince(province);
        delivery.setProductType(productType);
        delivery.setUnifiedDeliveryDriver("driver1");
        delivery.setRequestId("requestId");
        delivery.setNotificationSentAt("2023-10-01T12:00:00Z");
        delivery.setSenderPaId(senderPaId);
        delivery.setDeliveryDate("2023-10-02");
        delivery.setPriority(1);
        delivery.setAttempt(attempt);
        delivery.setDeliveryDate("2023-10-02");
        delivery.setPk("2023-10-02~EVALUATE_RESIDUAL_CAPACITY");
        return delivery;
    }
}

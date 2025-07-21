package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import it.pagopa.pn.delayer.utils.PnDelayerUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class EvaluateResidualCapacityJobServiceImpl implements EvaluateResidualCapacityJobService {

    private final PaperDeliveryUtils paperDeliveryUtils;
    private final PnDelayerUtils pnDelayerUtils;

    @Override
    public Mono<Void> startEvaluateResidualCapacityJob(String unifiedDeliveryDriver, String province, Instant startExecutionBatch, String tenderId) {
        LocalDate deliveryWeek = pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch);
        return paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY, unifiedDeliveryDriver, province, deliveryWeek, tenderId);
    }
}

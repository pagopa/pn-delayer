package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import java.time.LocalDate;

@Component
@Slf4j
@RequiredArgsConstructor
public class EvaluateResidualCapacityJobServiceImpl implements EvaluateResidualCapacityJobService {

    private final PaperDeliveryUtils paperDeliveryUtils;

    @Override
    public Mono<Void> startEvaluateResidualCapacityJob(String unifiedDeliveryDriver, String province, LocalDate deliveryWeek, String tenderId) {
        return paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY, unifiedDeliveryDriver, province, deliveryWeek, tenderId);
    }
}

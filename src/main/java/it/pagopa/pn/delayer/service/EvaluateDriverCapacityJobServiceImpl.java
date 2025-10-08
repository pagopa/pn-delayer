package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.LocalDate;

@Slf4j
@Component
@RequiredArgsConstructor
public class EvaluateDriverCapacityJobServiceImpl implements EvaluateDriverCapacityJobService {

    private final PaperDeliveryUtils paperDeliveryUtils;

    @Override
    public Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, LocalDate deliveryWeek, String tenderId) {
        return paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, unifiedDeliveryDriver, province, deliveryWeek, tenderId);
    }
}

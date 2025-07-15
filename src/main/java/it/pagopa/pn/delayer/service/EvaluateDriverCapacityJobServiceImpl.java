package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class EvaluateDriverCapacityJobServiceImpl implements EvaluateDriverCapacityJobService {

    private final PaperDeliveryService paperDeliveryService;
    private final PaperDeliveryUtils paperDeliveryUtils;

    @Override
    public Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch, String tenderId) {
        LocalDate deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(startExecutionBatch);
        return paperDeliveryService.evaluateCapacitiesAndProcessDeliveries(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, unifiedDeliveryDriver, province, lastEvaluatedKey, deliveryWeek, tenderId);
    }
}

package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.DeliveryDriverUtils;
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

@Slf4j
@Component
@RequiredArgsConstructor
public class EvaluateDriverCapacityJobServiceImpl implements EvaluateDriverCapacityJobService {

    private final PaperDeliveryUtils paperDeliveryUtils;
    private final PnDelayerUtils pnDelayerUtils;
    private final DeliveryDriverUtils deliveryDriverUtils;

    @Override
    public Mono<Void> startEvaluateDriverCapacityJob(String unifiedDeliveryDriver, String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecutionBatch, String tenderId) {
        LocalDate deliveryWeek = pnDelayerUtils.calculateDeliveryWeek(startExecutionBatch);
        return deliveryDriverUtils.retrieveDeclaredAndUsedCapacity(province, unifiedDeliveryDriver, tenderId, deliveryWeek)
                .doOnNext(tuple -> log.info("Retrieved capacities for province: [{}], unifiedDeliveryDriver: [{}] -> declared capacity={}, used capacity={}", province, unifiedDeliveryDriver, tuple.getT1(), tuple.getT2()))
                .flatMap(provinceCapacities -> paperDeliveryUtils.evaluateCapacitiesAndProcessDeliveries(provinceCapacities, WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, unifiedDeliveryDriver, province, lastEvaluatedKey, deliveryWeek, tenderId));
    }
}

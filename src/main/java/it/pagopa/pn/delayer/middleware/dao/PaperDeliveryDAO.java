package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface PaperDeliveryDAO {

    Mono<Page<PaperDelivery>> retrievePaperDeliveries(WorkflowStepEnum workflowStepEnum, LocalDate deliveryWeek, String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, Integer queryLimit);

    Mono<Void> insertPaperDeliveries(List<PaperDelivery> paperDeliveriesChunk);
}

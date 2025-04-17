package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

public interface PaperDeliveryHighPriorityDAO {

    Mono<Page<PaperDeliveryHighPriority>> getPaperDeliveryHighPriority(String unifiedDeliveryDriver, String geoKey, Map<String, AttributeValue> lastEvaluatedKey);

    Mono<Void> executeTransaction(List<PaperDeliveryHighPriority> paperDeliveryHighPriority, List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend);
}

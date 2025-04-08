package it.pagopa.pn.delayer.middleware.dao;


import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.model.PaperDeliveryTransactionRequest;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

public interface PaperDeliveryHighPriorityDAO {

    Mono<Integer> delete(String pk, List<PaperDeliveryHighPriority> highPriorities);
    List<PaperDeliveryHighPriority> get(String pk);
    Mono<Page<PaperDeliveryHighPriority>> getChunck(String pk, int limit, Map<String, AttributeValue> lastEvaluatedKey);

    Mono<Integer> executeTransaction(PaperDeliveryTransactionRequest paperDeliveryTransactionRequest);

}

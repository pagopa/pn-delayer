package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class PaperDeliveryDriverCapacitiesDAOImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final DynamoDbAsyncTable<PaperDeliveryDriverCapacity> table;

    public PaperDeliveryDriverCapacitiesDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.table = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName(), TableSchema.fromBean(PaperDeliveryDriverCapacity.class));
    }

    @Override
    public Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geoKey, Instant deliveryDate) {

        QueryConditional keyCondition = QueryConditional.sortLessThanOrEqualTo(Key.builder()
                .partitionValue(PaperDeliveryDriverCapacity.buildKey(tenderId, deliveryDriverId, geoKey))
                .sortValue(deliveryDate.toString()).build());

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":now", AttributeValue.builder().s(deliveryDate.toString()).build());

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#to", "activationDateTo");

        String filterExpression = "attribute_not_exists(" + PaperDeliveryDriverCapacity.COL_ACTIVATION_DATE_TO + ") OR #to >= :now";

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(keyCondition)
                .filterExpression(Expression.builder()
                        .expression(filterExpression)
                        .expressionValues(expressionValues)
                        .expressionNames(expressionAttributeNames)
                        .build())
                .limit(1)
                .scanIndexForward(false)
                .build();

        return Mono.from(table.query(queryRequest).items().limit(1))
                .map(PaperDeliveryDriverCapacity::getCapacity)
                .switchIfEmpty(Mono.just(0))
                .doOnError(e -> log.error("Error while querying PaperDeliveryDriverCapacities", e));
    }
}

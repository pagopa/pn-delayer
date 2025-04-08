package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacities;
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

    private final DynamoDbAsyncTable<PaperDeliveryDriverCapacities> table;

    public PaperDeliveryDriverCapacitiesDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.table = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName(), TableSchema.fromBean(PaperDeliveryDriverCapacities.class));
    }

    @Override
    public Mono<PaperDeliveryDriverCapacities> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geoKey) {
        Instant now = Instant.now();

        QueryConditional keyCondition = QueryConditional.sortLessThan(Key.builder()
                .partitionValue(PaperDeliveryDriverCapacities.buildKey(tenderId, deliveryDriverId, geoKey))
                .sortValue(now.toString()).build());

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":now", AttributeValue.builder().s(now.toString()).build());

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#to", "activationDateTo");

        String filterExpression = "attribute_not_exists(" + PaperDeliveryDriverCapacities.COL_ACTIVATION_DATE_TO + ") OR #to >= :now";

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

        return Mono.from(table.query(queryRequest).items());
    }
}

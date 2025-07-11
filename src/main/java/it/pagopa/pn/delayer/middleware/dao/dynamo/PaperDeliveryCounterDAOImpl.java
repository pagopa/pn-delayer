package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.Map;
import java.util.Objects;

@Component
@Slf4j
public class PaperDeliveryCounterDAOImpl implements PaperDeliveryCounterDAO {

    private final DynamoDbAsyncTable<PaperDeliveryCounter> tableCounter;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public PaperDeliveryCounterDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.tableCounter = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryCounterTableName(), TableSchema.fromBean(PaperDeliveryCounter.class));
    }

    public Mono<PaperDeliveryCounter> getPaperDeliveryCounter(String pk, String sk) {
        Key key = Key.builder()
                .partitionValue(pk)
                .sortValue(sk)
                .build();
        return Mono.fromFuture(tableCounter.getItem(key));
    }

    public Mono<Void> updatePrintCapacityCounter(String deliveryDate, Integer counter, Integer weeklyPrintCapacity, Integer excludedDeliveryCounter) {
        log.info("update print capacity counter for deliveryDate={} with weeklyPrintCapacity={} and field {} to increment of={}",
                deliveryDate, weeklyPrintCapacity, Objects.isNull(counter) ? "excludedDeliveryCounter" : "counter",
                Objects.isNull(counter) ? excludedDeliveryCounter : counter);

        Map<String, AttributeValue> key = Map.of(
                PaperDeliveryCounter.COL_DELIVERY_DATE, AttributeValue.builder().s(deliveryDate).build(),
                PaperDeliveryCounter.COL_SK, AttributeValue.builder().s("PRINT").build()
        );

        Map<String, AttributeValue> attributeValue = Map.of(
                ":weeklyPrintCapacity", AttributeValue.builder().n(String.valueOf(weeklyPrintCapacity)).build(),
                ":v", AttributeValue.builder().n(String.valueOf(counter == null ? excludedDeliveryCounter : counter)).build()
        );

        String updateExpression = Objects.isNull(counter)
                ? "ADD " + PaperDeliveryCounter.COL_COUNTER_EXCLUDED_DELIVERY_COUNTER + " :v SET " + PaperDeliveryCounter.COL_WEEKLY_PRINT_CAPACITY + " = :weeklyPrintCapacity"
                : "ADD " + PaperDeliveryCounter.COL_COUNTER + " :v SET " + PaperDeliveryCounter.COL_WEEKLY_PRINT_CAPACITY + " = :weeklyPrintCapacity";

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableCounter.tableName())
                .key(key)
                .updateExpression(updateExpression)
                .expressionAttributeValues(attributeValue)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .doOnSuccess(r -> log.info("Update print Capacity Counter successful for deliveryDate={}", deliveryDate))
                .doOnError(e -> log.error("Error updating print Capacity Counter for deliveryDate={}", deliveryDate))
                .then();
    }
}

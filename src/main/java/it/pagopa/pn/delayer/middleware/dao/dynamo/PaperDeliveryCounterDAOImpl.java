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
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class PaperDeliveryCounterDAOImpl implements PaperDeliveryCounterDAO {

    private final DynamoDbAsyncTable<PaperDeliveryCounter> tableCounter;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final PnDelayerConfigs pnDelayerConfigs;

    public PaperDeliveryCounterDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.tableCounter = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryCounterTableName(), TableSchema.fromBean(PaperDeliveryCounter.class));
        this.pnDelayerConfigs = pnDelayerConfigs;
    }

    public Mono<List<PaperDeliveryCounter>> getPaperDeliveryCounter(String pk, String sk) {
        QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.sortBeginsWith(Key.builder().partitionValue(pk)
                        .sortValue(sk).build()))
                .build();
        return Mono.from(tableCounter.query(queryEnhancedRequest).map(Page::items))
                .doOnError(error -> log.error("Error retrieving paper delivery counter for deliveryDate: {} and key: {}", pk, sk, error));
    }

    public Mono<Void> updatePrintCapacityCounter(LocalDate deliveryDate, Integer counter, Integer weeklyPrintCapacity) {
        log.info("update print capacity counter for deliveryDate={} with weeklyPrintCapacity={} and field counter to increment of={}",
                deliveryDate, weeklyPrintCapacity, counter);

        Map<String, AttributeValue> key = Map.of(
                PaperDeliveryCounter.COL_PK, AttributeValue.builder().s("PRINT").build(),
                PaperDeliveryCounter.COL_SK, AttributeValue.builder().s(deliveryDate.toString()).build()
        );

        Map<String, AttributeValue> attributeValue = Map.of(
                ":dailyPrintCapacity", AttributeValue.builder().n(String.valueOf(weeklyPrintCapacity / pnDelayerConfigs.getPrintCapacityWeeklyWorkingDays())).build(),
                ":weeklyPrintCapacity", AttributeValue.builder().n(String.valueOf(weeklyPrintCapacity)).build(),
                ":v", AttributeValue.builder().n(String.valueOf(counter)).build()
        );

        String updateExpression = "ADD " + PaperDeliveryCounter.COL_NUMBER_OF_SHIPMENTS + " :v SET " + PaperDeliveryCounter.COL_WEEKLY_PRINT_CAPACITY + " = :weeklyPrintCapacity,"
                + PaperDeliveryCounter.COL_DAILY_PRINT_CAPACITY + " = :dailyPrintCapacity";

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

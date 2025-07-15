package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryPrintCapacityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacity;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacityCounter;
import it.pagopa.pn.delayer.model.PrintCapacityEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_PRINT_CAPACITY_NOT_FOUND;
import static it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacityCounter.COL_TTL;

@Component
@Slf4j
public class PaperDeliveryPrintCapacityDAOImpl implements PaperDeliveryPrintCapacityDAO {

    private final DynamoDbAsyncTable<PaperDeliveryPrintCapacity> printCapacityTable;
    private final DynamoDbAsyncTable<PaperDeliveryPrintCapacityCounter> printCapacityCounterTable;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient;
    private final PnDelayerConfigs pnDelayerConfigs;

    public PaperDeliveryPrintCapacityDAOImpl(DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient, DynamoDbAsyncClient dynamoDbAsyncClient, PnDelayerConfigs pnDelayerConfigs) {
        this.printCapacityTable = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryPrintCapacityTableName(), TableSchema.fromBean(PaperDeliveryPrintCapacity.class));
        this.printCapacityCounterTable = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryPrintCapacityCounterTableName(), TableSchema.fromBean(PaperDeliveryPrintCapacityCounter.class));
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.pnDelayerConfigs = pnDelayerConfigs;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedAsyncClient;
    }

    @Override
    public Mono<Integer> retrieveActualPrintCapacity(LocalDate deliveryWeek) {
        QueryConditional keyCondition = QueryConditional.keyEqualTo(Key.builder().partitionValue("PRINT").build());

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(keyCondition)
                .limit(1)
                .scanIndexForward(false)
                .build();

        return Mono.from(printCapacityTable.query(queryRequest).items().limit(1))
                .map(PaperDeliveryPrintCapacity::getPrintCapacity)
                .switchIfEmpty(Mono.defer(() -> {
                    log.error("No Print capacity found");
                    return Mono.error(new PnInternalException("Print capacity not found", 404, ERROR_CODE_PRINT_CAPACITY_NOT_FOUND));
                }))
                .doOnError(e -> log.error("Error while querying PaperDeliveryDriverCapacities", e));
    }

    @Override
    public Mono<List<PaperDeliveryPrintCapacityCounter>> retrievePrintCapacityCounters(LocalDate deliveryWeek, LocalDate today) {
        Key dailyKey = Key.builder().partitionValue(today.toString()).sortValue(PrintCapacityEnum.DAILY.toString()).build();
        Key weeklyKey = Key.builder().partitionValue(deliveryWeek.toString()).sortValue(PrintCapacityEnum.WEEKLY.toString()).build();

        ReadBatch readBatchBuilder = ReadBatch
                .builder(PaperDeliveryPrintCapacityCounter.class)
                .addGetItem(dailyKey)
                .addGetItem(weeklyKey)
                .mappedTableResource(printCapacityCounterTable)
                .build();

        BatchGetItemEnhancedRequest request = BatchGetItemEnhancedRequest.builder()
                .addReadBatch(readBatchBuilder)
                .build();

        return Mono.from(dynamoDbEnhancedClient.batchGetItem(request))
                .map(batchGetResultPage -> batchGetResultPage.resultsForTable(printCapacityCounterTable))
                .doOnError(e -> log.error("Error retrieving weekly print capacity counter for week: {} and daily print capacity counter for days: {} --> {}",
                        deliveryWeek, today, e.getMessage()));

    }

    @Override
    public Mono<Void> updatePrintCapacity(PrintCapacityEnum printCapacityEnum, LocalDate deliveryWeek, Integer increment, Integer printCapacity) {
        log.info("updating {} print capacity for deliveryWeek: {} increment={}", printCapacityEnum.name(), deliveryWeek, increment);
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PaperDeliveryPrintCapacityCounter.COL_PK, AttributeValue.builder().s(deliveryWeek.toString()).build());
        key.put(PaperDeliveryPrintCapacityCounter.COL_SK, AttributeValue.builder().s(printCapacityEnum.toString()).build());

        long ttl = Instant.now().plus(pnDelayerConfigs.getDao().getPaperDeliveryPrintCapacityCounterTtlDuration()).toEpochMilli();

        Map<String, AttributeValue> attributeValue = new HashMap<>();
        attributeValue.put(":v", AttributeValue.builder().n(String.valueOf(increment)).build());
        attributeValue.put(":printCapacity", AttributeValue.builder().s(String.valueOf(printCapacity)).build());
        attributeValue.put(":ttl", AttributeValue.builder().n(String.valueOf(ttl)).build());

        Map<String, String> attributeName = new HashMap<>();
        attributeName.put("#ttlValue", COL_TTL);

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(printCapacityCounterTable.tableName())
                .key(key)
                .updateExpression("ADD " + PaperDeliveryPrintCapacityCounter.COL_USED_PRINT_CAPACITY + " :v" +
                        " SET " + PaperDeliveryPrintCapacityCounter.COL_PRINT_CAPACITY + " = :printCapacity, #ttlValue = :ttl")
                .expressionAttributeValues(attributeValue)
                .expressionAttributeNames(attributeName)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .then()
                .doOnError(e -> log.error("Error during update {} print capacity for deliveryWeek: {} increment={} --> {}",
                        printCapacityEnum.name(), deliveryWeek, increment, e.getMessage()));
    }
}

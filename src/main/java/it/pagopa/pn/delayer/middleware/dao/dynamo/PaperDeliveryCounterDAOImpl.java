package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
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

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

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

    public Mono<List<PaperDeliveryCounter>> getPaperDeliveryCounter(String pk, String sk, Integer limit) {

        QueryEnhancedRequest.Builder queryEnhancedRequestBuilder = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.sortBeginsWith(
                        Key.builder().partitionValue(pk).sortValue(sk).build()
                ));

        if (Objects.nonNull(limit)) {
            queryEnhancedRequestBuilder.limit(limit);
            queryEnhancedRequestBuilder.scanIndexForward(false);
        }
        QueryEnhancedRequest queryEnhancedRequest = queryEnhancedRequestBuilder.build();

        return Mono.from(tableCounter.query(queryEnhancedRequest).map(Page::items))
                .doOnError(error -> log.error("Error retrieving paper delivery counter for deliveryDate: {} and key: {}", pk, sk, error));
    }

    public Mono<Void> updatePrintCapacityCounter(LocalDate deliveryDate, Integer counter, Integer weeklyPrintCapacity) {
        log.info("update print capacity counter for deliveryDate={} with weeklyPrintCapacity={} and field counter to increment of={}",
                deliveryDate, weeklyPrintCapacity, counter);

        Map<String, String> expressionAttributeNames = new HashMap<>();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        List<String> updateExpressions = new ArrayList<>();

        Map<String, AttributeValue> map = PaperDeliveryCounter
                .entityToAttributeValueMap(PaperDeliveryCounter.constructPrintCounterEntity(weeklyPrintCapacity, pnDelayerConfigs.getPrintCapacityWeeklyWorkingDays(),
                        pnDelayerConfigs.getPrintCounterTtlDuration(), pnDelayerConfigs.calculateDailyExecutionNumber()));

        map.forEach((key, value) ->
                updateExpressions.add(buildUpdateExpressions(key, value, expressionAttributeNames, expressionAttributeValues)));

        String updateExpr = "SET " + String.join(", ", updateExpressions);
        updateExpr = updateExpr + " ADD " + PaperDeliveryCounter.COL_NUMBER_OF_SHIPMENTS + " :increment";
        expressionAttributeValues.put(":increment", AttributeValue.builder().n(String.valueOf(counter)).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableCounter.tableName())
                .key(Map.of("pk", AttributeValue.builder().s("PRINT").build(), "sk", AttributeValue.builder().s(deliveryDate.toString()).build()))
                .updateExpression(updateExpr)
                .expressionAttributeValues(expressionAttributeValues)
                .expressionAttributeNames(expressionAttributeNames)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .doOnSuccess(r -> log.info("Update print Capacity Counter successful for deliveryDate={}", deliveryDate))
                .doOnError(e -> log.error("Error updating print Capacity Counter for deliveryDate={} --> {}", deliveryDate, e.getMessage(), e))
                .then();
    }

    @Override
    public Mono<Void> updateExcludeCounter(LocalDate deliveryWeek, Map<String, Long> excludeGroupedRecords) {
        if (CollectionUtils.isEmpty(excludeGroupedRecords)) {
            return Mono.empty();
        }

        long ttl = Instant.now().plus(pnDelayerConfigs.getPrintCounterTtlDuration()).toEpochMilli();

        return Flux.fromIterable(excludeGroupedRecords.entrySet())
                .concatMap(entry -> {
                    String sk = "EXCLUDE~" + entry.getKey();

                    Map<String, String> expressionAttributeNames = new HashMap<>();
                    expressionAttributeNames.put("#numberOfShipments", PaperDeliveryCounter.COL_NUMBER_OF_SHIPMENTS);
                    expressionAttributeNames.put("#ttl", PaperDeliveryCounter.COL_TTL);

                    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                    expressionAttributeValues.put(":inc", AttributeValue.builder().n(String.valueOf(entry.getValue())).build());
                    expressionAttributeValues.put(":ttl", AttributeValue.builder().n(String.valueOf(ttl)).build());

                    UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                            .tableName(tableCounter.tableName())
                            .key(Map.of(
                                    PaperDeliveryCounter.COL_PK, AttributeValue.builder().s(deliveryWeek.toString()).build(),
                                    PaperDeliveryCounter.COL_SK, AttributeValue.builder().s(sk).build()
                            ))
                            .updateExpression("ADD #numberOfShipments :inc SET #ttl = :ttl")
                            .expressionAttributeValues(expressionAttributeValues)
                            .expressionAttributeNames(expressionAttributeNames)
                            .build();

                    return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                            .doOnSuccess(r -> log.info("Update exclude counter successful for sk {}", sk))
                            .doOnError(e -> log.error("Error updating exclude counter for sk: {}", sk, e))
                            .then();
                })
                .then();
    }

    private String buildUpdateExpressions(String key, AttributeValue value, Map<String, String> names, Map<String, AttributeValue> values) {
        if(key.equalsIgnoreCase(PaperDeliveryCounter.COL_NUMBER_OF_SHIPMENTS)) {
            return "";
        }
        names.put("#" + key, key);
        values.put(":" + key, value);
        return "#" + key + " = " + ":" + key;
    }
}

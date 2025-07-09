package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveriesSenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveriesSenderLimit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveriesSenderLimit.COL_PERCENTAGE_LIMIT;

@Component
@Slf4j
public class PaperDeliveriesSenderLimitDAOImpl implements PaperDeliveriesSenderLimitDAO {

    private final DynamoDbAsyncTable<PaperDeliveriesSenderLimit> table;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public PaperDeliveriesSenderLimitDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.table = dynamoDbEnhancedClient.table(pnDelayerConfigs.getDao().getPaperDeliverySenderLimitTableName(), TableSchema.fromBean(PaperDeliveriesSenderLimit.class));
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
    }

    @Override
    public Flux<PaperDeliveriesSenderLimit> batchGetItem(List<String> pks, Instant deliveryDate) {
        log.info("batchGetItem for senderLimits pks={} deliveryDate={}", pks, deliveryDate);

        List<Key> keys = pks.stream()
                .map(pk -> Key.builder()
                        .partitionValue(pk)
                        .sortValue(deliveryDate.toString())
                        .build())
                .toList();

        ReadBatch.Builder<PaperDeliveriesSenderLimit> readBatchBuilder = ReadBatch
                .builder(PaperDeliveriesSenderLimit.class)
                .mappedTableResource(table);

        keys.forEach(readBatchBuilder::addGetItem);
        ReadBatch readBatch = readBatchBuilder.build();

        BatchGetItemEnhancedRequest request = BatchGetItemEnhancedRequest.builder()
                .addReadBatch(readBatch)
                .build();

        return Mono.from(dynamoDbEnhancedClient.batchGetItem(request))
                .map(batchGetResultPage -> batchGetResultPage.resultsForTable(table))
                .doOnNext(items -> log.info("Retrieved senderLimits items: {}", items.size()))
                .flatMapMany(Flux::fromIterable)
                .doOnError(e -> log.error("Error retrieving senderLimits items with pks {}: {}", pks, e.getMessage()));
    }

    @Override
    public Mono<Integer> updatePercentageLimit(String pk, Integer increment, Instant deliveryDate) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PaperDeliveriesSenderLimit.COL_PK, AttributeValue.builder().s(pk).build());
        key.put(PaperDeliveriesSenderLimit.COL_DELIVERY_DATE, AttributeValue.builder().s(deliveryDate.toString()).build());

        Map<String, AttributeValue> attributeValue = new HashMap<>();
        attributeValue.put(":v", AttributeValue.builder().n(String.valueOf(increment)).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(table.tableName())
                .key(key)
                .updateExpression("ADD " + COL_PERCENTAGE_LIMIT + " :v")
                .expressionAttributeValues(attributeValue)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .thenReturn(increment)
                .doOnSuccess(r -> log.info("Update successful for pk={} increment={}", pk, increment))
                .doOnError(e -> log.error("Error updating item with pk {}: {}", pk, e.getMessage()));
    }
}

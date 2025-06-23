package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverUsedCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import it.pagopa.pn.delayer.model.ImplementationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;
import static it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities.*;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.DYNAMO, matchIfMissing = true)
public class PaperDeliveryDriverUsedUsedCapacitiesDAOImpl implements PaperDeliveryDriverUsedCapacitiesDAO {

    private final DynamoDbAsyncTable<PaperDeliveryDriverUsedCapacities> table;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient;

    public PaperDeliveryDriverUsedUsedCapacitiesDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbAsyncClient dynamoDbAsyncClient, DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient) {
        this.table = dynamoDbEnhancedClient.table(pnDelayerConfigs.getDao().getPaperDeliveryDriverUsedCapacitiesTableName(), TableSchema.fromBean(PaperDeliveryDriverUsedCapacities.class));
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
    }

    @Override
    public Mono<Integer> updateCounter(String unifiedDeliveryDriver, String geoKey, Integer increment, Instant deliveryDate) {
        String pk = PaperDeliveryDriverUsedCapacities.buildPk(unifiedDeliveryDriver, geoKey);
        log.info("update pk={} increment={}", pk, increment);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PaperDeliveryDriverUsedCapacities.COL_UNIFIED_DELIVERY_DRIVER_GEOKEY, AttributeValue.builder().s(pk).build());
        key.put(PaperDeliveryDriverUsedCapacities.COL_DELIVERY_DATE, AttributeValue.builder().s(deliveryDate.toString()).build());

        Map<String, AttributeValue> attributeValue = new HashMap<>();
        attributeValue.put(":v", AttributeValue.builder().n(String.valueOf(increment)).build());
        attributeValue.put(":deliveryDriver", AttributeValue.builder().s(unifiedDeliveryDriver).build());
        attributeValue.put(":geoKey", AttributeValue.builder().s(geoKey).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(table.tableName())
                .key(key)
                .updateExpression("ADD " + COL_USED_CAPACITY + " :v" +
                        " SET " + COL_UNIFIED_DELIVERY_DRIVER + " = :deliveryDriver," + COL_GEO_KEY + "= :geoKey")
                .expressionAttributeValues(attributeValue)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .thenReturn(increment)
                .doOnSuccess(r -> log.info("Update successful for pk={} increment={}", pk, increment))
                .doOnError(e -> log.error("Error updating item with pk {}: {}", pk, e.getMessage()));
    }

    @Override
    public Mono<Integer> get(String unifiedDeliveryDriver, String geoKey, Instant deliveryDate) {
        String pk = PaperDeliveryDriverUsedCapacities.buildPk(unifiedDeliveryDriver, geoKey);
        return Mono.fromFuture(table.getItem(Key.builder()
                        .partitionValue(pk)
                        .sortValue(String.valueOf(deliveryDate))
                        .build()))
                .map(PaperDeliveryDriverUsedCapacities::getUsedCapacity)
                .switchIfEmpty(Mono.just(0))
                .doOnError(e -> log.error("Error retrieving usedCapacity item with pk {}: {}", pk, e.getMessage()));
    }

    @Override
    public Flux<PaperDeliveryDriverUsedCapacities> batchGetItem(List<String> pks, Instant deliveryDate) {
        log.info("batchGetItem for usedCapacity pks={} deliveryDate={}", pks, deliveryDate);

        List<Key> keys = pks.stream()
                .map(pk -> Key.builder()
                        .partitionValue(pk)
                        .sortValue(deliveryDate.toString())
                        .build())
                .toList();

        ReadBatch.Builder<PaperDeliveryDriverUsedCapacities> readBatchBuilder = ReadBatch
                .builder(PaperDeliveryDriverUsedCapacities.class)
                .mappedTableResource(table);

        keys.forEach(readBatchBuilder::addGetItem);
        ReadBatch readBatch = readBatchBuilder.build();

        BatchGetItemEnhancedRequest request = BatchGetItemEnhancedRequest.builder()
                .addReadBatch(readBatch)
                .build();

        return Mono.from(dynamoDbEnhancedClient.batchGetItem(request))
                .map(batchGetResultPage -> batchGetResultPage.resultsForTable(table))
                .doOnNext(items -> log.info("Retrieved usedCapacity items: {}", items.size()))
                .flatMapMany(Flux::fromIterable)
                .doOnError(e -> log.error("Error retrieving usedCapacity items with pks {}: {}", pks, e.getMessage()));
    }
}


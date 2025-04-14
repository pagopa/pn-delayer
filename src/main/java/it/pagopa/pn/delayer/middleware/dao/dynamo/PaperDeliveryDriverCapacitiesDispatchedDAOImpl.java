package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
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
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched.*;

@Component
@Slf4j
public class PaperDeliveryDriverCapacitiesDispatchedDAOImpl implements PaperDeliveryDriverCapacitiesDispatchedDAO {

    private final DynamoDbAsyncTable<PaperDeliveryDriverCapacitiesDispatched> table;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient;

    public PaperDeliveryDriverCapacitiesDispatchedDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbAsyncClient dynamoDbAsyncClient, DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient) {
        this.table = dynamoDbEnhancedClient.table(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesDispatchedTableName(), TableSchema.fromBean(PaperDeliveryDriverCapacitiesDispatched.class));
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
    }

    @Override
    public Mono<UpdateItemResponse> updateCounter(String deliveryDriverId, String geoKey, Integer increment, Instant deliveryDate) {
        String pk = PaperDeliveryDriverCapacitiesDispatched.buildPk(deliveryDriverId, geoKey);
        log.info("update pk={} increment={}", pk, increment);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PaperDeliveryDriverCapacitiesDispatched.COL_DELIVERY_DRIVER_ID_GEOKEY, AttributeValue.builder().s(pk).build());
        key.put(PaperDeliveryDriverCapacitiesDispatched.COL_DELIVERY_DATE, AttributeValue.builder().s(deliveryDate.toString()).build());

        Map<String, AttributeValue> attributeValue = new HashMap<>();
        attributeValue.put(":v", AttributeValue.builder().n(String.valueOf(increment)).build());
        attributeValue.put(":deliveryDriver", AttributeValue.builder().s(deliveryDriverId).build());
        attributeValue.put(":geoKey", AttributeValue.builder().s(geoKey).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(table.tableName())
                .key(key)
                .updateExpression("ADD " + COL_USED_CAPACITY + " :v" +
                        " SET " + COL_DELIVERY_DRIVER_ID + " = :deliveryDriver," + COL_GEO_KEY + "= :geoKey")
                .expressionAttributeValues(attributeValue)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .doOnSuccess(r -> log.info("Update successful for pk={} increment={}", pk, increment))
                .doOnError(e -> log.error("Error updating item with pk {}: {}", pk, e.getMessage()));
    }

    @Override
    public Mono<Integer> get(String deliveryDriverId, String geoKey, Instant deliveryDate) {
        String pk = PaperDeliveryDriverCapacitiesDispatched.buildPk(deliveryDriverId, geoKey);
        return Mono.fromFuture(table.getItem(Key.builder()
                        .partitionValue(pk)
                        .sortValue(String.valueOf(deliveryDate))
                        .build()))
                .map(PaperDeliveryDriverCapacitiesDispatched::getUsedCapacity)
                .switchIfEmpty(Mono.just(0))
                .doOnSuccess(item -> log.info("Retrieved used capacity for pk [{}] and deliveryWeek [{}] = {}",pk, deliveryDate, item))
                .doOnError(e -> log.error("Error retrieving usedCapacity item with pk {}: {}", pk, e.getMessage()));
    }

    @Override
    public Flux<PaperDeliveryDriverCapacitiesDispatched> batchGetItem(List<String> pks, Instant deliveryDate) {
        log.info("batchGetItem for usedCapacity pks={} deliveryDate={}", pks, deliveryDate);

        List<Key> keys = pks.stream()
                .map(pk -> Key.builder()
                        .partitionValue(pk)
                        .sortValue(deliveryDate.toString())
                        .build())
                .toList();

        ReadBatch.Builder<PaperDeliveryDriverCapacitiesDispatched> readBatchBuilder = ReadBatch
                .builder(PaperDeliveryDriverCapacitiesDispatched.class)
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


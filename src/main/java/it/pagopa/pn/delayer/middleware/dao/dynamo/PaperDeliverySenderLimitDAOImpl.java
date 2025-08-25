package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryUsedSenderLimit;
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

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class PaperDeliverySenderLimitDAOImpl implements PaperDeliverySenderLimitDAO {

    private final DynamoDbAsyncTable<PaperDeliverySenderLimit> senderLimitTable;
    private final DynamoDbAsyncTable<PaperDeliveryUsedSenderLimit> usedSenderLimitTable;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public PaperDeliverySenderLimitDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.senderLimitTable = dynamoDbEnhancedClient.table(pnDelayerConfigs.getDao().getPaperDeliverySenderLimitTableName(), TableSchema.fromBean(PaperDeliverySenderLimit.class));
        this.usedSenderLimitTable = dynamoDbEnhancedClient.table(pnDelayerConfigs.getDao().getPaperDeliveryUsedSenderLimitTableName(), TableSchema.fromBean(PaperDeliveryUsedSenderLimit.class));
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
    }

    @Override
    public Flux<PaperDeliverySenderLimit> retrieveSendersLimit(List<String> pks, LocalDate shipmentDate) {
        log.info("retrieve sender Limit for tuples={} on shipmentDate={}", pks, shipmentDate);

        List<Key> keys = pks.stream()
                .map(pk -> Key.builder()
                        .partitionValue(pk)
                        .sortValue(shipmentDate.toString())
                        .build())
                .toList();

        ReadBatch.Builder<PaperDeliverySenderLimit> readBatchBuilder = ReadBatch
                .builder(PaperDeliverySenderLimit.class)
                .mappedTableResource(senderLimitTable);

        keys.forEach(readBatchBuilder::addGetItem);
        ReadBatch readBatch = readBatchBuilder.build();

        BatchGetItemEnhancedRequest request = BatchGetItemEnhancedRequest.builder()
                .addReadBatch(readBatch)
                .build();

        return Mono.from(dynamoDbEnhancedClient.batchGetItem(request))
                .map(batchGetResultPage -> batchGetResultPage.resultsForTable(senderLimitTable))
                .doOnNext(items -> log.info("Retrieved senderLimits [{}] items", items.size()))
                .flatMapMany(Flux::fromIterable)
                .doOnError(e -> log.error("Error retrieving senderLimits items with pks {}: {}", pks, e.getMessage()));
    }

    @Override
    public Mono<Long> updateUsedSenderLimit(String pk, Long increment, LocalDate shipmentDate, Integer senderLimit) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PaperDeliverySenderLimit.COL_PK, AttributeValue.builder().s(pk).build());
        key.put(PaperDeliverySenderLimit.COL_DELIVERY_DATE, AttributeValue.builder().s(shipmentDate.toString()).build());

        Map<String, AttributeValue> attributeValue = new HashMap<>();
        attributeValue.put(":v", AttributeValue.builder().n(String.valueOf(increment)).build());
        attributeValue.put(":senderLimit", AttributeValue.builder().n(String.valueOf(senderLimit)).build());
        attributeValue.put(":paId", AttributeValue.builder().s(pk.split("~")[0]).build());
        attributeValue.put(":province", AttributeValue.builder().s(pk.split("~")[2]).build());
        attributeValue.put(":productType", AttributeValue.builder().s(pk.split("~")[1]).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(usedSenderLimitTable.tableName())
                .key(key)
                .updateExpression("ADD " + PaperDeliveryUsedSenderLimit.COL_NUMBER_OF_SHIPMENT + " :v"+
                        " SET " + PaperDeliveryUsedSenderLimit.COL_SENDER_LIMIT + " = :senderLimit, "
                        + PaperDeliveryUsedSenderLimit.COL_PAID + " = :paId, "
                        + PaperDeliveryUsedSenderLimit.COL_PROVINCE + " = :province, "
                        + PaperDeliveryUsedSenderLimit.COL_PRODUCT_TYPE + " = :productType")
                .expressionAttributeValues(attributeValue)
                .build();

        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(updateRequest))
                .thenReturn(increment)
                .doOnSuccess(r -> log.info("Update successful for pk={} increment={}", pk, increment))
                .doOnError(e -> log.error("Error updating item with pk {}: {}", pk, e.getMessage()));
    }

    @Override
    public Flux<PaperDeliveryUsedSenderLimit> retrieveUsedSendersLimit(List<String> pks, LocalDate shipmentDate) {
        log.info("retrieve used sender Limit for tuples={} on shipmentDate={}", pks, shipmentDate);

        List<Key> keys = pks.stream()
                .map(pk -> Key.builder()
                        .partitionValue(pk)
                        .sortValue(shipmentDate.toString())
                        .build())
                .toList();

        ReadBatch.Builder<PaperDeliveryUsedSenderLimit> readBatchBuilder = ReadBatch
                .builder(PaperDeliveryUsedSenderLimit.class)
                .mappedTableResource(usedSenderLimitTable);

        keys.forEach(readBatchBuilder::addGetItem);
        ReadBatch readBatch = readBatchBuilder.build();

        BatchGetItemEnhancedRequest request = BatchGetItemEnhancedRequest.builder()
                .addReadBatch(readBatch)
                .build();

        return Mono.from(dynamoDbEnhancedClient.batchGetItem(request))
                .map(batchGetResultPage -> batchGetResultPage.resultsForTable(usedSenderLimitTable))
                .doOnNext(items -> log.info("Retrieved used senderLimits [{}] items", items.size()))
                .flatMapMany(Flux::fromIterable)
                .doOnError(e -> log.error("Error retrieving usedSenderLimits items with pks {}: {}", pks, e.getMessage()));
    }
}

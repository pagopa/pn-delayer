package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.model.ImplementationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.DYNAMO, matchIfMissing = true)
public class PaperDeliveryHighPriorityDAOImpl implements PaperDeliveryHighPriorityDAO {

    private final PnDelayerConfigs pnDelayerConfigs;
    private final DynamoDbAsyncTable<PaperDeliveryHighPriority> tableHighPriority;
    private final DynamoDbAsyncTable<PaperDeliveryReadyToSend> tableReadyToSend;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;

    public PaperDeliveryHighPriorityDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.pnDelayerConfigs = pnDelayerConfigs;
        this.tableHighPriority = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryHighPriorityTableName(), TableSchema.fromBean(PaperDeliveryHighPriority.class));
        this.tableReadyToSend = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryReadyToSendTableName(), TableSchema.fromBean(PaperDeliveryReadyToSend.class));
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
    }

    @Override
    public Mono<Page<PaperDeliveryHighPriority>> getPaperDeliveryHighPriority(String unifiedDeliveryDriver, String geoKey, Map<String, AttributeValue> lastEvaluatedKey) {
        QueryConditional keyCondition = QueryConditional.keyEqualTo(Key.builder()
                .partitionValue(PaperDeliveryHighPriority.buildKey(unifiedDeliveryDriver, geoKey))
                .build());

        QueryEnhancedRequest.Builder requestBuilder = QueryEnhancedRequest.builder()
                .queryConditional(keyCondition)
                .limit(pnDelayerConfigs.getHighPriorityQueryLimit());

        if (!CollectionUtils.isEmpty(lastEvaluatedKey)) {
            requestBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        return Mono.from(tableHighPriority.query(requestBuilder.build()));
    }

    @Override
    public Mono<Void> executeTransaction(List<PaperDeliveryHighPriority> paperDeliveryHighPriority, List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend) {
        TransactWriteItemsEnhancedRequest.Builder transactWriteItemsEnhancedRequest = TransactWriteItemsEnhancedRequest.builder();

        if (!CollectionUtils.isEmpty(paperDeliveryHighPriority)) {
            paperDeliveryHighPriority.forEach(item -> transactWriteItemsEnhancedRequest.addDeleteItem(tableHighPriority, item));
        }
        if (!CollectionUtils.isEmpty(paperDeliveryReadyToSend)) {
            paperDeliveryReadyToSend.forEach(item -> transactWriteItemsEnhancedRequest.addPutItem(tableReadyToSend, item));
        }
        return Mono.fromFuture(dynamoDbEnhancedAsyncClient.transactWriteItems(transactWriteItemsEnhancedRequest.build()));
    }
}

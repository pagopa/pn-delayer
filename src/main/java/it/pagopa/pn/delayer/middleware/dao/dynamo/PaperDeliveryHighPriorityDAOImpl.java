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
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;
import static it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority.*;
import static it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend.COL_DELIVERY_DATE;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primaryPartitionKey;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primarySortKey;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.DYNAMO, matchIfMissing = true)
public class PaperDeliveryHighPriorityDAOImpl implements PaperDeliveryHighPriorityDAO {

    private final PnDelayerConfigs pnDelayerConfigs;
    private final DynamoDbAsyncTable<PaperDeliveryHighPriority> tableHighPriority;
    private final DynamoDbAsyncTable<PaperDeliveryReadyToSend> tableReadyToSend;
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;

    public PaperDeliveryHighPriorityDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        StaticTableSchema<PaperDeliveryHighPriority> staticTable = StaticTableSchema.builder(PaperDeliveryHighPriority.class)
                .newItemSupplier(PaperDeliveryHighPriority::new)
                .addAttribute(String.class, a -> a.name(COL_UNIFIED_DELIVERY_DRIVER_GEOKEY)
                        .getter(PaperDeliveryHighPriority::getUnifiedDeliveryDriverGeoKey)
                        .setter(PaperDeliveryHighPriority::setUnifiedDeliveryDriverGeoKey)
                        .tags(primaryPartitionKey())
                )
                .addAttribute(Instant.class, a -> a.name(COL_CREATED_AT)
                        .getter(PaperDeliveryHighPriority::getCreatedAt)
                        .setter(PaperDeliveryHighPriority::setCreatedAt)
                        .tags(primarySortKey())
                )
                .addAttribute(String.class, a -> a.name(COL_TENDER_ID)
                        .getter(PaperDeliveryHighPriority::getTenderId)
                        .setter(PaperDeliveryHighPriority::setTenderId)
                )
                .addAttribute(String.class, a -> a.name(COL_REQUEST_ID)
                        .getter(PaperDeliveryHighPriority::getRequestId)
                        .setter(PaperDeliveryHighPriority::setRequestId)
                )
                .addAttribute(String.class, a -> a.name(COL_UNIFIED_DELIVERY_DRIVER)
                        .getter(PaperDeliveryHighPriority::getUnifiedDeliveryDriver)
                        .setter(PaperDeliveryHighPriority::setUnifiedDeliveryDriver)
                )
                .addAttribute(String.class, a -> a.name(COL_PROVINCE)
                        .getter(PaperDeliveryHighPriority::getProvince)
                        .setter(PaperDeliveryHighPriority::setProvince)
                )
                .addAttribute(String.class, a -> a.name(COL_PRODUCT_TYPE)
                        .getter(PaperDeliveryHighPriority::getProductType)
                        .setter(PaperDeliveryHighPriority::setProductType)
                )
                .addAttribute(String.class, a -> a.name(COL_SENDER_PA_ID)
                        .getter(PaperDeliveryHighPriority::getSenderPaId)
                        .setter(PaperDeliveryHighPriority::setSenderPaId)
                )
                .addAttribute(String.class, a -> a.name(COL_RECIPIENT_ID)
                        .getter(PaperDeliveryHighPriority::getRecipientId)
                        .setter(PaperDeliveryHighPriority::setRecipientId)
                )
                .addAttribute(String.class, a -> a.name(COL_CAP)
                        .getter(PaperDeliveryHighPriority::getCap)
                        .setter(PaperDeliveryHighPriority::setCap)
                )
                .addAttribute(String.class, a -> a.name(COL_IUN)
                        .getter(PaperDeliveryHighPriority::getIun)
                        .setter(PaperDeliveryHighPriority::setIun)
                )
                .build();
        StaticTableSchema<PaperDeliveryReadyToSend> staticTableTwo = StaticTableSchema.builder(PaperDeliveryReadyToSend.class)
                .newItemSupplier(PaperDeliveryReadyToSend::new)
                .addAttribute(Instant.class, a -> a.name(COL_DELIVERY_DATE)
                        .getter(PaperDeliveryReadyToSend::getDeliveryDate)
                        .setter(PaperDeliveryReadyToSend::setDeliveryDate)
                        .tags(primaryPartitionKey())
                )
                .addAttribute(String.class, a -> a.name(PaperDeliveryReadyToSend.COL_REQUEST_ID)
                        .getter(PaperDeliveryReadyToSend::getRequestId)
                        .setter(PaperDeliveryReadyToSend::setRequestId)
                        .tags(primarySortKey())
                )
                .addAttribute(String.class, a -> a.name(PaperDeliveryReadyToSend.COL_IUN)
                        .getter(PaperDeliveryReadyToSend::getIun)
                        .setter(PaperDeliveryReadyToSend::setIun)
                )
                .build();
        this.pnDelayerConfigs = pnDelayerConfigs;
        this.tableHighPriority = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryHighPriorityTableName(), staticTable);
        this.tableReadyToSend = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryReadyToSendTableName(), staticTableTwo);
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

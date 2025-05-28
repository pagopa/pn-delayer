package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.ImplementationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primaryPartitionKey;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primarySortKey;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.DYNAMO, matchIfMissing = true)
public class PaperDeliveryDriverCapacitiesDAOImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final DynamoDbAsyncTable<PaperDeliveryDriverCapacity> table;

    public PaperDeliveryDriverCapacitiesDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        //todo add addAttribute for staticTableSchema
        StaticTableSchema<PaperDeliveryDriverCapacity> schemaTable = StaticTableSchema.builder(PaperDeliveryDriverCapacity.class)
                .newItemSupplier(PaperDeliveryDriverCapacity::new)
                .addAttribute(String.class, a -> a.name(PaperDeliveryDriverCapacity.COL_PK)
                        .getter(PaperDeliveryDriverCapacity::getPk)
                        .setter(PaperDeliveryDriverCapacity::setPk)
                        .tags(primaryPartitionKey())
                )
                .addAttribute(Instant.class, a -> a.name(PaperDeliveryDriverCapacity.COL_ACTIVATION_DATE_FROM)
                        .getter(PaperDeliveryDriverCapacity::getActivationDateFrom)
                        .setter(PaperDeliveryDriverCapacity::setActivationDateFrom)
                        .tags(primarySortKey())
                )
                .addAttribute(Instant.class, a -> a.name(PaperDeliveryDriverCapacity.COL_ACTIVATION_DATE_TO)
                        .getter(PaperDeliveryDriverCapacity::getActivationDateTo)
                        .setter(PaperDeliveryDriverCapacity::setActivationDateTo)
                )
                .addAttribute(String.class, a -> a.name(PaperDeliveryDriverCapacity.COL_TENDER_ID)
                        .getter(PaperDeliveryDriverCapacity::getTenderId)
                        .setter(PaperDeliveryDriverCapacity::setTenderId)
                )
                .addAttribute(String.class, a -> a.name(PaperDeliveryDriverCapacity.COL_UNIFIED_DELIVERY_DRIVER)
                        .getter(PaperDeliveryDriverCapacity::getUnifiedDeliveryDriver)
                        .setter(PaperDeliveryDriverCapacity::setUnifiedDeliveryDriver)
                )
                .addAttribute(String.class, a -> a.name(PaperDeliveryDriverCapacity.COL_GEO_KEY)
                        .getter(PaperDeliveryDriverCapacity::getGeoKey)
                        .setter(PaperDeliveryDriverCapacity::setGeoKey)
                )
                .addAttribute(Integer.class, a -> a.name(PaperDeliveryDriverCapacity.COL_CAPACITY)
                        .getter(PaperDeliveryDriverCapacity::getCapacity)
                        .setter(PaperDeliveryDriverCapacity::setCapacity)
                )
                .addAttribute(Integer.class, a -> a.name(PaperDeliveryDriverCapacity.COL_PEAK_CAPACITY)
                        .getter(PaperDeliveryDriverCapacity::getPeakCapacity)
                        .setter(PaperDeliveryDriverCapacity::setPeakCapacity)
                )
                .addAttribute(Instant.class, a -> a.name(PaperDeliveryDriverCapacity.COL_CREATED_AT)
                        .getter(PaperDeliveryDriverCapacity::getCreatedAt)
                        .setter(PaperDeliveryDriverCapacity::setCreatedAt)
                )
                .build();
        this.table = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName(), schemaTable);
    }

    @Override
    public Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String unifiedDeliveryDriver, String geoKey, Instant deliveryDate) {

        QueryConditional keyCondition = QueryConditional.sortLessThanOrEqualTo(Key.builder()
                .partitionValue(PaperDeliveryDriverCapacity.buildKey(tenderId, unifiedDeliveryDriver, geoKey))
                .sortValue(deliveryDate.toString()).build());

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":now", AttributeValue.builder().s(deliveryDate.toString()).build());

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#to", "activationDateTo");

        String filterExpression = "attribute_not_exists(" + PaperDeliveryDriverCapacity.COL_ACTIVATION_DATE_TO + ") OR #to >= :now";

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(keyCondition)
                .filterExpression(Expression.builder()
                        .expression(filterExpression)
                        .expressionValues(expressionValues)
                        .expressionNames(expressionAttributeNames)
                        .build())
                .limit(1)
                .scanIndexForward(false)
                .build();

        return Mono.from(table.query(queryRequest).items().limit(1))
                .map(PaperDeliveryDriverCapacity::getCapacity)
                .switchIfEmpty(Mono.defer(() -> {
                    log.error("No PaperDeliveryDriverCapacity found for tenderId: {}, unifiedDeliveryDriver: {}, geoKey: {}, deliveryDate: {}",
                            tenderId, unifiedDeliveryDriver, geoKey, deliveryDate);
                    return Mono.just(0);
                }))
                .doOnError(e -> log.error("Error while querying PaperDeliveryDriverCapacities", e));
    }
}

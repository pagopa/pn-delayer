package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class PaperDeliveryDriverCapacitiesDAOImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final DynamoDbAsyncTable<PaperDeliveryDriverCapacity> table;

    public PaperDeliveryDriverCapacitiesDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.table = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryDriverCapacitiesTableName(), TableSchema.fromBean(PaperDeliveryDriverCapacity.class));
    }

    @Override
    public Mono<Integer> getPaperDeliveryDriverCapacities(String tenderId, String unifiedDeliveryDriver, String geoKey, LocalDate deliveryDate) {

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

    @Override
    public Mono<List<PaperDeliveryDriverCapacity>> retrieveUnifiedDeliveryDriversOnProvince(String tenderId, String geoKey, LocalDate deliveryDate) {

        QueryConditional keyCondition = QueryConditional.sortLessThanOrEqualTo(Key.builder()
                .partitionValue(PaperDeliveryDriverCapacity.buildGsiKey(tenderId, geoKey))
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
                .scanIndexForward(false)
                .limit(1000)
                .build();

        return Flux.from(table.index(PaperDeliveryDriverCapacity.TENDER_ID_GEO_KEY_INDEX).query(queryRequest))
                .flatMap(page -> Flux.fromIterable(page.items()))
                .groupBy(PaperDeliveryDriverCapacity::getUnifiedDeliveryDriver)
                .flatMap(groupedFlux ->
                        groupedFlux.reduce((a, b) ->
                                a.getActivationDateFrom().isAfter(b.getActivationDateFrom()) ? a : b
                        )
                )
                .collectList()
                .doOnError(e -> log.error("Error during retrieve PaperDeliveryDriverCapacities for tenderId: {}, geoKey: {}, deliveryDate: {}", tenderId, geoKey, deliveryDate, e));
    }
}

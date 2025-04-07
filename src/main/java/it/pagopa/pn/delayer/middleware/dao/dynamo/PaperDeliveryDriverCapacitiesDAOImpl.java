package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@Component
@Slf4j
public class PaperDeliveryDriverCapacitiesDAOImpl implements PaperDeliveryDriverCapacitiesDAO {

    private final DynamoDbAsyncTable<PaperDeliveryDriverCapacities> table;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public PaperDeliveryDriverCapacitiesDAOImpl(DynamoDbAsyncTable<PaperDeliveryDriverCapacities> table, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.table = table;
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    }

    @Override
    public Mono<PaperDeliveryDriverCapacities> getPaperDeliveryDriverCapacities(String tenderId, String deliveryDriverId, String geokey) {
        log.info("getPaperDeliveryDriverCapacities for tenderId={}, deliveryDriverId={}, geokey={}", tenderId, deliveryDriverId, geokey);

        String pk = tenderId + "##" + deliveryDriverId + "##" + geokey;
        Key hashKey = Key.builder()
                .partitionValue(pk)
                .build();

        return Mono.fromFuture(table.getItem(hashKey))
                .doOnSuccess(item -> log.info("Retrieved item: {}", item))
                .doOnError(error -> log.error("Failed to retrieve item with key={}", pk, error));
    }
}

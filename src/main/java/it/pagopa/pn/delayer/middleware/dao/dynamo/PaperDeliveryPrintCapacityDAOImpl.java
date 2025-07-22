package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryPrintCapacityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryPrintCapacity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;

import java.time.LocalDate;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_PRINT_CAPACITY_NOT_FOUND;

@Component
@Slf4j
public class PaperDeliveryPrintCapacityDAOImpl implements PaperDeliveryPrintCapacityDAO {

    private final DynamoDbAsyncTable<PaperDeliveryPrintCapacity> printCapacityTable;

    public PaperDeliveryPrintCapacityDAOImpl(DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient, PnDelayerConfigs pnDelayerConfigs) {
        this.printCapacityTable = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryPrintCapacityTableName(), TableSchema.fromBean(PaperDeliveryPrintCapacity.class));
    }

    @Override
    public Mono<Integer> retrieveActualPrintCapacity(LocalDate deliveryWeek) {
        QueryConditional keyCondition = QueryConditional.keyEqualTo(Key.builder().partitionValue("PRINT").build());

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(keyCondition)
                .limit(1)
                .scanIndexForward(false)
                .build();

        return Mono.from(printCapacityTable.query(queryRequest).items().limit(1))
                .map(PaperDeliveryPrintCapacity::getPrintCapacity)
                .switchIfEmpty(Mono.defer(() -> {
                    log.error("No Print capacity found");
                    return Mono.error(new PnInternalException("Print capacity not found", 404, ERROR_CODE_PRINT_CAPACITY_NOT_FOUND));
                }))
                .doOnError(e -> log.error("Error while querying PaperDeliveryDriverCapacities", e));
    }
}

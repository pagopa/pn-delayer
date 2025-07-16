package it.pagopa.pn.delayer.middleware.dao.dynamo;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_INSERT_PAPER_DELIVERY_ENTITY;

@Component
@Slf4j
public class PaperDeliveryDAOImpl implements PaperDeliveryDAO {

    private final DynamoDbAsyncTable<PaperDelivery> table;
    private final DynamoDbEnhancedAsyncClient enhancedAsyncClient;

    public PaperDeliveryDAOImpl(PnDelayerConfigs pnDelayerConfigs, DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.table = dynamoDbEnhancedAsyncClient.table(pnDelayerConfigs.getDao().getPaperDeliveryTableName(), TableSchema.fromBean(PaperDelivery.class));
        this.enhancedAsyncClient = dynamoDbEnhancedAsyncClient;
    }

    @Override
    public Mono<Page<PaperDelivery>> retrievePaperDeliveries(WorkflowStepEnum workflowStepEnum, LocalDate deliveryWeek, String sortKeyPrefix, Map<String, AttributeValue> lastEvaluatedKey, Integer queryLimit) {
        QueryConditional keyCondition = QueryConditional.sortBeginsWith(Key.builder()
                .partitionValue(String.join("~", deliveryWeek.toString(), workflowStepEnum.name()))
                .sortValue(sortKeyPrefix)
                .build());

        QueryEnhancedRequest.Builder requestBuilder = QueryEnhancedRequest.builder()
                .queryConditional(keyCondition)
                .limit(queryLimit);

        if (!CollectionUtils.isEmpty(lastEvaluatedKey)) {
            requestBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        return Mono.from(table.query(requestBuilder.build()));
    }

    @Override
    public Mono<Void> insertPaperDeliveries(List<PaperDelivery> paperDeliveries) {
        return Flux.fromIterable(paperDeliveries).buffer(25)
                .flatMap(chunk -> insertWithRetry(chunk, 3))
                .then();
    }

    private Mono<Void> insertWithRetry(List<PaperDelivery> paperDeliveriesChunk, int retriesLeft) {
        BatchWriteItemEnhancedRequest.Builder batchBuilder = BatchWriteItemEnhancedRequest.builder();
        paperDeliveriesChunk.forEach(paperDelivery -> {
            WriteBatch writeBatch = WriteBatch.builder(PaperDelivery.class)
                    .mappedTableResource(table)
                    .addPutItem(paperDelivery)
                    .build();
            batchBuilder.addWriteBatch(writeBatch);
        });

        return Mono.fromFuture(enhancedAsyncClient.batchWriteItem(batchBuilder.build()))
            .flatMap(response -> {
                List<PaperDelivery> unprocessed = response.unprocessedPutItemsForTable(table);
                if (unprocessed != null && !unprocessed.isEmpty()) {
                    if (retriesLeft > 1) {
                        log.info("Retrying batch write for {} unprocessed items, {} retries left", unprocessed.size(), retriesLeft - 1);
                        return insertWithRetry(unprocessed, retriesLeft - 1);
                    } else {
                        log.error("Failed to insert PaperDelivery after 3 attempts, unprocessed items remain: {}", unprocessed.size());
                        return Mono.error(new PnInternalException("Error during insert PaperDelivery, Unprocessed items remain after 3 attempts", ERROR_CODE_INSERT_PAPER_DELIVERY_ENTITY));
                    }
                }
                return Mono.empty();
            });
    }
}

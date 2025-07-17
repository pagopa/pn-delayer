package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.cache.CapProductTypeDriverCacheService;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class PaperDeliverySenderLimitDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliverySenderLimitDAO paperDeliveriesSenderLimitDAO;

    @Autowired
    DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    PnDelayerConfigs pnDelayerConfigs;

    @Test
    void testRetrieveSendersLimit() {
        List<String> pks = List.of("0~RS~RM", "1~RS~RM", "2~RS~RM");

        IntStream.range(0, 3).forEach(i -> {
            PaperDeliverySenderLimit paperDeliveriesSenderLimit = new PaperDeliverySenderLimit();
            paperDeliveriesSenderLimit.setPk(i + "~RS~RM");
            paperDeliveriesSenderLimit.setDeliveryDate("2025-04-07");
            Map<String, AttributeValue> itemMap = new HashMap<>();
            itemMap.put("pk", AttributeValue.builder().s(paperDeliveriesSenderLimit.getPk()).build());
            itemMap.put("deliveryDate", AttributeValue.builder().s(paperDeliveriesSenderLimit.getDeliveryDate()).build());
            Mono.fromFuture(dynamoDbAsyncClient.putItem(PutItemRequest.builder().item(itemMap).tableName(pnDelayerConfigs.getDao().getPaperDeliverySenderLimitTableName()).build())).block();
        });


        StepVerifier.create(paperDeliveriesSenderLimitDAO.retrieveSendersLimit(pks, LocalDate.parse("2025-04-07")))
                .expectNextCount(3)
                .expectComplete()
                .verify();
    }

    @Test
    void testUpdateNumberOfShipment() {
        PaperDeliveryDriverUsedCapacities entity = new PaperDeliveryDriverUsedCapacities();
        LocalDate dateTime = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(1)));
        entity.setUnifiedDeliveryDriverGeokey("1~RM");
        entity.setDeliveryDate(nextWeek);

        paperDeliveriesSenderLimitDAO.updateUsedSenderLimit("1~RS~RM", 5, nextWeek, 1000).block();

        int response = get(nextWeek);
        assert response != 0;
        Assertions.assertEquals(5, response);

        paperDeliveriesSenderLimitDAO.updateUsedSenderLimit("1~RS~RM", 5, nextWeek, 1000).block();

        int response2 = get(nextWeek);
        assert response2 != 0;
        Assertions.assertEquals(10, response2);

    }

    private int get(LocalDate deliveryDate) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("pk", AttributeValue.builder().s("1~RS~RM").build());
        key.put("deliveryDate", AttributeValue.builder().s(String.valueOf(deliveryDate)).build());

        GetItemRequest request = GetItemRequest.builder()
                .tableName(pnDelayerConfigs.getDao().getPaperDeliveryUsedSenderLimitTableName())
                .key(key)
                .build();

        GetItemResponse response = Mono.fromFuture(dynamoDbAsyncClient.getItem(request)).block();
        Assertions.assertNotNull(response);
        return response.item().containsKey("numberOfShipment")
                ? Integer.parseInt(response.item().get("numberOfShipment").n())
                : 0;
    }
}

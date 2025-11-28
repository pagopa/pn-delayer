package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@DynamoDbBean
@Data
public class PaperDeliveryCounter {
    public static final String COL_PK = "pk";
    public static final String COL_SK = "sk";
    public static final String COL_NUMBER_OF_SHIPMENTS = "numberOfShipments";
    public static final String COL_TTL = "ttl";
    public static final String COL_WEEKLY_PRINT_CAPACITY = "weeklyPrintCapacity";
    public static final String COL_DAILY_PRINT_CAPACITY = "dailyPrintCapacity";
    public static final String COL_SENT_TO_NEXT_WEEK = "sentToNextWeek";
    public static final String COL_SENT_TO_PHASE_TWO = "sentToPhaseTwo";
    public static final String COL_LEK_TO_NEXT_WEEK = "lastEvaluatedKeyNextWeek";
    public static final String COL_LEK_PHASE2 = "lastEvaluatedKeyPhase2";
    public static final String COL_STOP_SEND_TO_PHASE_TWO = "stopSendToPhaseTwo";
    public static final String COL_DAILY_EXECUTION_NUMBER = "dailyExecutionNumber";
    public static final String COL_DAILY_EXECUTION_COUNTER = "dailyExecutionCounter";


    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_SK)}))
    private String sk;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_NUMBER_OF_SHIPMENTS)}))
    private Integer numberOfShipments;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DAILY_PRINT_CAPACITY)}))
    private Integer dailyPrintCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_WEEKLY_PRINT_CAPACITY)}))
    private Integer weeklyPrintCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DAILY_EXECUTION_NUMBER)}))
    private Integer dailyExecutions;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DAILY_EXECUTION_COUNTER)}))
    private Integer dailyExecutionCounter;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_SENT_TO_NEXT_WEEK)}))
    private Integer sentToNextWeek;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_SENT_TO_PHASE_TWO)}))
    private Integer sentToPhaseTwo;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_STOP_SEND_TO_PHASE_TWO)}))
    private Boolean stopSendToPhaseTwo;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TTL)}))
    private long ttl;

    @DynamoDbIgnore
    public static String retrieveProductFromSk(String sk) {
        return sk.split("~")[2];
    }

    @DynamoDbIgnore
    public static Map<String, AttributeValue> entityToAttributeValueMap(PaperDeliveryCounter paperDeliveryCounter) {
        var schema = TableSchema.fromBean(PaperDeliveryCounter.class);
        Map<String, AttributeValue> map = new HashMap<>(schema.itemToMap(paperDeliveryCounter, true));
        map.put(PaperDeliveryCounter.COL_LEK_TO_NEXT_WEEK, AttributeValue.builder().m(Map.of()).build());
        map.put(PaperDeliveryCounter.COL_LEK_PHASE2, AttributeValue.builder().m(Map.of()).build());
        return map;
    }

    @DynamoDbIgnore
    public static PaperDeliveryCounter constructPrintCounterEntity(Integer weeklyPrintCapacity, Integer workingDays, Duration ttlDuration, Integer dailyExecutionNumber) {
        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setDailyPrintCapacity(weeklyPrintCapacity / workingDays);
        paperDeliveryCounter.setWeeklyPrintCapacity(weeklyPrintCapacity);
        paperDeliveryCounter.setSentToNextWeek(0);
        paperDeliveryCounter.setSentToPhaseTwo(0);
        paperDeliveryCounter.setDailyExecutionCounter(0);
        paperDeliveryCounter.setDailyExecutions(dailyExecutionNumber);
        paperDeliveryCounter.setTtl(Instant.now().plus(ttlDuration).toEpochMilli());
        paperDeliveryCounter.setStopSendToPhaseTwo(false);
        return paperDeliveryCounter;
    }

    @DynamoDbIgnore
    public static String buildSkPrefix(SkPrefix skPrefix, String... attribute) {
        if(Objects.nonNull(skPrefix)) {
            return skPrefix.value + String.join("~", attribute).concat("~");
        }
        return String.join("~", attribute);
    }

    public enum SkPrefix {
        SUM_ESTIMATES("SUM_ESTIMATES~"),
        EXCLUDE("EXCLUDE~");

        private final String value;

        SkPrefix(String value) {
            this.value = value;
        }
    }
}

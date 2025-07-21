package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.time.LocalDate;

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
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_SENT_TO_NEXT_WEEK)}))
    private Integer sentToNextWeek;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TTL)}))
    private long ttl;
}

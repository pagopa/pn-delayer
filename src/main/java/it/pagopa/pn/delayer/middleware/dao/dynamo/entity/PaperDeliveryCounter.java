package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.time.LocalDate;

@DynamoDbBean
@Data
public class PaperDeliveryCounter {
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_SK = "sk";
    public static final String COL_COUNTER = "counterValue";
    public static final String COL_TTL = "ttl";
    public static final String COL_WEEKLY_PRINT_CAPACITY = "weeklyPrintCapacity";
    public static final String COL_COUNTER_EXCLUDED_DELIVERY_COUNTER = "excludedDeliveryCounter";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private LocalDate deliveryDate;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_SK)}))
    private String sk;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_COUNTER)}))
    private Integer counter;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_WEEKLY_PRINT_CAPACITY)}))
    private Integer weeklyPrintCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_COUNTER_EXCLUDED_DELIVERY_COUNTER)}))
    private Integer excludedDeliveryCounter;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TTL)}))
    private long ttl;

    @DynamoDbIgnore
    public static String retrieveProductFromSk(String sk) {
        return sk.split("~")[2];
    }
}

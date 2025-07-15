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
public class PaperDeliveryPrintCapacityCounter {
    public static final String COL_PK = "pk";
    public static final String COL_SK = "sk";
    public static final String COL_PRINT_CAPACITY = "printCapacity";
    public static final String COL_USED_PRINT_CAPACITY = "usedPrintCapacity";
    public static final String COL_TTL = "ttl";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private LocalDate pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_SK)}))
    private String sk;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_USED_PRINT_CAPACITY)}))
    private Integer usedPrintCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRINT_CAPACITY)}))
    private Integer printCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TTL)}))
    private long ttl;
}

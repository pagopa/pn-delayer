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
public class PaperDeliveryPrintCapacity {

    public static final String COL_PK = "pk";
    public static final String COL_START_DATE = "startDate";
    public static final String COL_PRINT_CAPACITY = "printCapacity";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_START_DATE)}))
    private LocalDate startDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRINT_CAPACITY)}))
    private Integer printCapacity;
}
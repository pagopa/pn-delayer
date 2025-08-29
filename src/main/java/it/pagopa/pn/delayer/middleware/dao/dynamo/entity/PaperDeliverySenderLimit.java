package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
public class PaperDeliverySenderLimit {
    public static final String COL_PK = "pk";
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_WEEKLY_ESTIMATE = "weeklyEstimate";
    public static final String COL_MONTHLY_ESTIMATE = "monthlyEstimate";
    public static final String COL_ORIGINAL_ESTIMATE = "originalEstimate";
    public static final String COL_PAID = "paId";
    public static final String COL_PRODUCT_TYPE = "productType";
    public static final String COL_PROVINCE = "province";
    public static final String COL_TTL = "ttl";


    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private String deliveryDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_WEEKLY_ESTIMATE)}))
    private int weeklyEstimate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_MONTHLY_ESTIMATE)}))
    private double monthlyEstimate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_ORIGINAL_ESTIMATE)}))
    private Integer originalEstimate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PAID)}))
    private String paId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRODUCT_TYPE)}))
    private String productType;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PROVINCE)}))
    private String province;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TTL)}))
    private int ttl;
}

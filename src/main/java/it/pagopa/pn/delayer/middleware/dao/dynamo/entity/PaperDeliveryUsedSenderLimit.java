package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
public class PaperDeliveryUsedSenderLimit {
    public static final String COL_PK = "pk";
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_PAID = "paId";
    public static final String COL_PRODUCT_TYPE = "productType";
    public static final String COL_PROVINCE = "province";
    public static final String COL_TTL = "ttl";
    public static final String COL_NUMBER_OF_SHIPMENT = "numberOfShipment";
    public static final String COL_SENDER_LIMIT = "senderLimit";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private String deliveryDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PAID)}))
    private String paId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRODUCT_TYPE)}))
    private String productType;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PROVINCE)}))
    private String province;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_NUMBER_OF_SHIPMENT)}))
    private Integer numberOfShipment;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_SENDER_LIMIT)}))
    private Integer senderLimit;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TTL)}))
    private int ttl;
}

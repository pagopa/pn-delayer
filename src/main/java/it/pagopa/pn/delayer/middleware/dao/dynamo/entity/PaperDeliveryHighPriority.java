package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.time.Instant;

@DynamoDbBean
@Data
public class PaperDeliveryHighPriority {

    public static final String COL_UNIFIED_DELIVERY_DRIVER_GEOKEY = "unifiedDeliveryDriverGeokey";
    public static final String COL_CREATED_AT = "createdAt";
    public static final String COL_TENDER_ID = "tenderId";
    public static final String COL_REQUEST_ID = "requestId";
    public static final String COL_UNIFIED_DELIVERY_DRIVER = "unifiedDeliveryDriver";
    public static final String COL_PROVINCE = "province";
    public static final String COL_PRODUCT_TYPE = "productType";
    public static final String COL_SENDER_PA_ID = "senderPaId";
    public static final String COL_RECIPIENT_ID = "recipientId";
    public static final String COL_CAP = "cap";
    public static final String COL_IUN = "iun";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_UNIFIED_DELIVERY_DRIVER_GEOKEY)}))
    private String unifiedDeliveryDriverGeoKey;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_CREATED_AT)}))
    private Instant createdAt;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID)}))
    private String tenderId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_REQUEST_ID)}))
    private String requestId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_UNIFIED_DELIVERY_DRIVER)}))
    private String unifiedDeliveryDriver;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PROVINCE)}))
    private String province;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRODUCT_TYPE)}))
    private String productType;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_SENDER_PA_ID)}))
    private String senderPaId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_RECIPIENT_ID)}))
    private String recipientId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CAP)}))
    private String cap;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_IUN)}))
    private String iun;

    @DynamoDbIgnore
    public static String buildKey(String unifiedDeliveryDriver, String geokey) {
        return String.join("~", unifiedDeliveryDriver, geokey);
    }

    @DynamoDbIgnore
    public static String retrieveunifiedDeliveryDriver(String unifiedDeliveryDriverGeokey) {
        return unifiedDeliveryDriverGeokey.split("~")[0];
    }

    @DynamoDbIgnore
    public static String retrieveGeoKey(String unifiedDeliveryDriverGeokey) {
        return unifiedDeliveryDriverGeokey.split("~")[1];
    }
}

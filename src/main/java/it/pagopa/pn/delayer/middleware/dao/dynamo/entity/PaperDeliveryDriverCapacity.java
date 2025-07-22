package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.time.Instant;
import java.util.List;

@DynamoDbBean
@Data
public class PaperDeliveryDriverCapacity {

    public static final String COL_PK = "pk";
    public static final String COL_ACTIVATION_DATE_FROM = "activationDateFrom";
    public static final String COL_ACTIVATION_DATE_TO = "activationDateTo";
    public static final String COL_TENDER_ID = "tenderId";
    public static final String COL_UNIFIED_DELIVERY_DRIVER = "unifiedDeliveryDriver";
    public static final String COL_GEO_KEY = "geoKey";
    public static final String COL_CAPACITY = "capacity";
    public static final String COL_PEAK_CAPACITY = "peakCapacity";
    public static final String COL_CREATED_AT = "createdAt";
    public static final String COL_TENDER_ID_GEO_KEY = "tenderIdGeoKey";
    public static final String COL_PRODUCTS = "products";

    public static final String TENDER_ID_GEO_KEY_INDEX = "tenderIdGeoKey-index";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_ACTIVATION_DATE_FROM),@DynamoDbSecondarySortKey(indexNames = TENDER_ID_GEO_KEY_INDEX)}))
    private Instant activationDateFrom;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_ACTIVATION_DATE_TO)}))
    private Instant activationDateTo;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID)}))
    private String tenderId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_UNIFIED_DELIVERY_DRIVER)}))
    private String unifiedDeliveryDriver;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_GEO_KEY)}))
    private String geoKey;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CAPACITY)}))
    private int capacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PEAK_CAPACITY)}))
    private int peakCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CREATED_AT)}))
    private Instant createdAt;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID_GEO_KEY), @DynamoDbSecondaryPartitionKey(indexNames = TENDER_ID_GEO_KEY_INDEX)}))
    private String tenderIdGeoKey;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRODUCTS)}))
    private List<String> products;

    @DynamoDbIgnore
    public static String buildKey(String tenderId, String deliveryDriver, String geokey) {
        return String.join("~", tenderId, deliveryDriver, geokey);
    }
    @DynamoDbIgnore
    public static String buildGsiKey(String tenderId, String geokey) {
        return String.join("~", tenderId, geokey);
    }
}

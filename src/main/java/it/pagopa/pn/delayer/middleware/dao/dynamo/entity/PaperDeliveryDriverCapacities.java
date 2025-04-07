package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
public class PaperDeliveryDriverCapacities {

    public static final String COL_PK = "deliveryDriverIdGeokey";
    public static final String COL_ACTIVATION_DATE_FROM = "activationDateFrom";
    public static final String COL_ACTIVATION_DATE_TO = "activationDateTo";
    public static final String COL_TENDER_ID = "tenderId";
    public static final String COL_DELIVERY_DRIVER_ID = "deliveryDriverId";
    public static final String COL_GEO_KEY = "geoKey";
    public static final String COL_CAPACITY = "capacity";
    public static final String COL_PEAK_CAPACITY = "peakCapacity";
    public static final String COL_CREATED_AT = "createdAt";

    public PaperDeliveryDriverCapacities() {
    }

    public PaperDeliveryDriverCapacities(String deliveryDriverIdGeokey, String deliveryDate, String tenderId, String deliveryDriverId, String geokey, int capacity, int peakCapacity, String createdAt) {
        this.pk = deliveryDriverIdGeokey;
        this.activationDateFrom = deliveryDate;
        this.activationDateTo = deliveryDate;
        this.tenderId = tenderId;
        this.deliveryDriverId = deliveryDriverId;
        this.geoKey = geokey;
        this.capacity = capacity;
        this.peakCapacity = peakCapacity;
        this.createdAt = createdAt;
    }

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_ACTIVATION_DATE_FROM)}))
    private String activationDateFrom;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_ACTIVATION_DATE_TO)}))
    private String activationDateTo;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID)}))
    private String tenderId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DELIVERY_DRIVER_ID)}))
    private String deliveryDriverId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_GEO_KEY)}))
    private String geoKey;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CAPACITY)}))
    private int capacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PEAK_CAPACITY)}))
    private int peakCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CREATED_AT)}))
    private String createdAt;
}

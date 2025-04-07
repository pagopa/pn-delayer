package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
@Builder
public class PaperDeliveryDriverCapacitiesDispatched {

    public static final String COL_DELIVERY_DRIVER_ID_GEOKEY = "deliveryDriverIdGeokey";
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_TENDER_ID = "tenderId";
    public static final String COL_DELIVERY_DRIVER_ID = "deliveryDriverId";
    public static final String COL_GEO_KEY = "geoKey";
    public static final String COL_CAPACITY = "capacity";

    public PaperDeliveryDriverCapacitiesDispatched() {
    }

    public PaperDeliveryDriverCapacitiesDispatched(String deliveryDriverIdGeokey, String deliveryDate, String tenderId, String deliveryDriverId, String geokey, int capacity) {
        this.deliveryDriverIdGeokey = deliveryDriverIdGeokey;
        this.deliveryDate = deliveryDate;
        this.tenderId = tenderId;
        this.deliveryDriverId = deliveryDriverId;
        this.geokey = geokey;
        this.capacity = capacity;
    }

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_DELIVERY_DRIVER_ID_GEOKEY)}))
    private String deliveryDriverIdGeokey;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private String deliveryDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID)}))
    private String tenderId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DELIVERY_DRIVER_ID)}))
    private String deliveryDriverId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_GEO_KEY)}))
    private String geokey;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CAPACITY)}))
    private int capacity;
}

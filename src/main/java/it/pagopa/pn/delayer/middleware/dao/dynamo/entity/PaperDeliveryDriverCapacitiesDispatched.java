package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.time.Instant;

@DynamoDbBean
@Data
public class PaperDeliveryDriverCapacitiesDispatched {

    public static final String COL_DELIVERY_DRIVER_ID_GEOKEY = "deliveryDriverIdGeokey";
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_TENDER_ID = "tenderId";
    public static final String COL_DELIVERY_DRIVER_ID = "deliveryDriverId";
    public static final String COL_GEO_KEY = "geoKey";
    public static final String COL_CAPACITY = "usedCapacity";


    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_DELIVERY_DRIVER_ID_GEOKEY)}))
    private String deliveryDriverIdGeokey;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private Instant deliveryDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID)}))
    private String tenderId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DELIVERY_DRIVER_ID)}))
    private String deliveryDriverId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_GEO_KEY)}))
    private String geoKey;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CAPACITY)}))
    private int usedCapacity;

    public static String buildPk(String deliveryDriverId, String geoKey) {
        return String.join("##", deliveryDriverId, geoKey);
    }
}

package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
public class PaperDeliveryDriverUsedCapacities {

    public static final String COL_UNIFIED_DELIVERY_DRIVER_GEOKEY = "unifiedDeliveryDriverGeokey";
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_UNIFIED_DELIVERY_DRIVER = "unifiedDeliveryDriver";
    public static final String COL_GEO_KEY = "geoKey";
    public static final String COL_USED_CAPACITY = "usedCapacity";
    public static final String COL_DECLARED_CAPACITY = "declaredCapacity";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_UNIFIED_DELIVERY_DRIVER_GEOKEY)}))
    private String unifiedDeliveryDriverGeokey;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private String deliveryDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_UNIFIED_DELIVERY_DRIVER)}))
    private String unifiedDeliveryDriver;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_GEO_KEY)}))
    private String geoKey;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_USED_CAPACITY)}))
    private int usedCapacity;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DECLARED_CAPACITY)}))
    private int declaredCapacity;

    public static String buildPk(String unifiedDeliveryDriver, String geoKey) {
        return String.join("~", unifiedDeliveryDriver, geoKey);
    }
}

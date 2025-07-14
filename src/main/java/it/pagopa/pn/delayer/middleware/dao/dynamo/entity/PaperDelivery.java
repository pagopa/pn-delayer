package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

@DynamoDbBean
@Data
public class PaperDelivery {

    public static final String COL_PK = "pk";
    public static final String COL_SK = "sk";
    public static final String COL_CREATED_AT = "createdAt";
    public static final String COL_REQUEST_ID = "requestId";
    public static final String COL_NOTIFICATION_SENT_AT = "notificationSentAt";
    public static final String COL_PREPARE_REQUEST_DATE = "prepareRequestDate";
    public static final String COL_PRODUCT_TYPE = "productType";
    public static final String COL_SENDER_PA_ID = "senderPaId";
    public static final String COL_PROVINCE = "province";
    public static final String COL_CAP = "cap";
    public static final String COL_ATTEMPT = "attempt";
    public static final String COL_IUN = "iun";
    public static final String COL_UNIFIED_DELIVERY_DRIVER = "unifiedDeliveryDriver";
    public static final String COL_TENDER_ID = "tenderId";
    public static final String COL_PRIORITY = "priority";
    public static final String COL_RECIPIENT_ID = "recipientId";
    public static final String COL_DELIVERY_DATE = "deliveryDate";

    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_PK)}))
    private String pk;
    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_SK)}))
    private String sk;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_REQUEST_ID)}))
    private String requestId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CREATED_AT)}))
    private String createdAt;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_NOTIFICATION_SENT_AT)}))
    private String notificationSentAt;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PREPARE_REQUEST_DATE)}))
    private String prepareRequestDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRODUCT_TYPE)}))
    private String productType;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_SENDER_PA_ID)}))
    private String senderPaId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PROVINCE)}))
    private String province;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_CAP)}))
    private String cap;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_ATTEMPT)}))
    private Integer attempt;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_IUN)}))
    private String iun;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_UNIFIED_DELIVERY_DRIVER)}))
    private String unifiedDeliveryDriver;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_TENDER_ID)}))
    private String tenderId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_PRIORITY)}))
    private Integer priority;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_RECIPIENT_ID)}))
    private String recipientId;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private String deliveryDate;

    @DynamoDbIgnore
    public static String buildSortKey(String... fields) {
        return String.join("~", fields);
    }

    @DynamoDbIgnore
    public static String buildPk(WorkflowStepEnum workflowStepEnum, String deliveryWeek) {
        return String.join( "~", deliveryWeek, workflowStepEnum.name());
    }
}

package it.pagopa.pn.delayer.middleware.dao.dynamo.entity;

import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.Data;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.time.Instant;
import java.time.LocalDate;

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
    public static final String COL_WORKFLOW_STEP = "workflowStep";

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
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_WORKFLOW_STEP)}))
    private String workflowStep;

    public PaperDelivery(){}

    public PaperDelivery(PaperDelivery paperDelivery, WorkflowStepEnum workflowStepEnum, LocalDate deliveryWeek){
        this.pk = buildPk(workflowStepEnum, deliveryWeek);
        this.sk = buildSortKey(workflowStepEnum, paperDelivery);
        this.requestId = paperDelivery.getRequestId();
        this.createdAt = String.valueOf(Instant.now());
        this.notificationSentAt = paperDelivery.getNotificationSentAt();
        this.prepareRequestDate = paperDelivery.getPrepareRequestDate();
        this.productType = paperDelivery.getProductType();
        this.senderPaId = paperDelivery.getSenderPaId();
        this.province = paperDelivery.getProvince();
        this.cap = paperDelivery.getCap();
        this.attempt = paperDelivery.getAttempt();
        this.iun = paperDelivery.getIun();
        this.unifiedDeliveryDriver = paperDelivery.getUnifiedDeliveryDriver();
        this.tenderId = paperDelivery.getTenderId();
        this.priority = paperDelivery.getPriority();
        this.recipientId = paperDelivery.getRecipientId();
        this.deliveryDate = paperDelivery.getDeliveryDate();
        this.workflowStep = workflowStepEnum.name();
    }

    @DynamoDbIgnore
    public static String buildSortKey(WorkflowStepEnum workflowStepEnum, PaperDelivery paperDelivery) {
        String date =  paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1 ?
                paperDelivery.getPrepareRequestDate() : paperDelivery.getNotificationSentAt();
        return switch (workflowStepEnum) {
            case EVALUATE_SENDER_LIMIT ->
                    String.join("~", paperDelivery.getProvince(), date, paperDelivery.getRequestId());
            case EVALUATE_DRIVER_CAPACITY ->
                    String.join("~", paperDelivery.getUnifiedDeliveryDriver(), paperDelivery.getProvince(), String.valueOf(paperDelivery.getPriority()), date, paperDelivery.getRequestId());
            case EVALUATE_RESIDUAL_CAPACITY ->
                    String.join("~", paperDelivery.getUnifiedDeliveryDriver(), paperDelivery.getProvince(), date, paperDelivery.getRequestId());
            case EVALUATE_PRINT_CAPACITY ->
                    String.join("~", String.valueOf(paperDelivery.getPriority()), date, paperDelivery.getRequestId());
            case SENT_TO_PREPARE_PHASE_2 ->
                    String.join("~", paperDelivery.getDeliveryDate(), paperDelivery.getRequestId());
        };
    }

    @DynamoDbIgnore
    public static String buildPk(WorkflowStepEnum workflowStepEnum, LocalDate deliveryWeek) {
        return String.join( "~", deliveryWeek.toString(), workflowStepEnum.name());
    }
}

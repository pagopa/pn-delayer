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
public class PaperDeliveryReadyToSend {

    public static final String COL_REQUEST_ID = "requestId";
    public static final String COL_DELIVERY_DATE = "deliveryDate";
    public static final String COL_IUN = "iun";

    @Getter(onMethod = @__({@DynamoDbSortKey, @DynamoDbAttribute(COL_REQUEST_ID)}))
    private String requestId;
    @Getter(onMethod = @__({@DynamoDbPartitionKey, @DynamoDbAttribute(COL_DELIVERY_DATE)}))
    private Instant deliveryDate;
    @Getter(onMethod = @__({@DynamoDbAttribute(COL_IUN)}))
    private String iun;
}

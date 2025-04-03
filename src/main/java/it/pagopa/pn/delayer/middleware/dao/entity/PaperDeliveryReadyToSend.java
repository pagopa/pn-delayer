package it.pagopa.pn.delayer.middleware.dao.entity;

import lombok.Data;

import java.time.Instant;

@Data
public class PaperDeliveryReadyToSend {
    //pk deliveryDate##requestId
    private String pk; //fixed value
    private String requestId;
    private Instant createdAt;
    private String iun;
}

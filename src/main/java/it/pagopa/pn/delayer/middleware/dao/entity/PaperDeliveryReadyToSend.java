package it.pagopa.pn.delayer.middleware.dao.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Builder
@Getter
@Setter
public class PaperDeliveryReadyToSend {
    //pk deliveryDate##requestId
    private Instant deliveryDate;
    private String requestId;
    private String iun;
    private String deliveryDriverId;
    private String province;
}

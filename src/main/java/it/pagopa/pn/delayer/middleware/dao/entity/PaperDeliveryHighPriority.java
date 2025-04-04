package it.pagopa.pn.delayer.middleware.dao.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaperDeliveryHighPriority {
    // pk DeliveryDriverId##province

    private String pk;
    private Instant createdAt;
    private String tenderId;
    private String requestId;
    private String deliveryDriverId;
    private String province;
    private String productType;
    private String senderPaId;
    private String cap;
    private String iun;


}

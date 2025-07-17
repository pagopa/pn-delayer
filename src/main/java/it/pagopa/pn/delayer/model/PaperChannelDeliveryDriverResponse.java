package it.pagopa.pn.delayer.model;

import lombok.Data;

@Data
public class PaperChannelDeliveryDriverResponse {
    private String geoKey;
    private String product;
    private String unifiedDeliveryDriver;
}

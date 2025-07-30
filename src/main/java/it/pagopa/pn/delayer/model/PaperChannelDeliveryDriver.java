package it.pagopa.pn.delayer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaperChannelDeliveryDriver {
    private String geoKey;
    private String product;
    private String unifiedDeliveryDriver;
}

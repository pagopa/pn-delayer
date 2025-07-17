package it.pagopa.pn.delayer.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeliveryDriverRequest {

    private String geoKey;
    private String product;
}

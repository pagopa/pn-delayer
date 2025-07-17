package it.pagopa.pn.delayer.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class PaperChannelDeliveryDriverRequest {

    private List<DeliveryDriverRequest> requests;
    private String tenderId;
    private String operation;
}

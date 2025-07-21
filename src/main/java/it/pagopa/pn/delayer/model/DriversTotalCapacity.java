package it.pagopa.pn.delayer.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class DriversTotalCapacity {

    private List<String> products;
    private Integer capacity;
    private List<String> unifiedDeliveryDrivers;
}

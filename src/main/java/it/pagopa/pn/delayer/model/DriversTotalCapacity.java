package it.pagopa.pn.delayer.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class DriversTotalCapacity {

    private Integer capacity;
    private List<String> unifiedDeliveryDrivers;
}

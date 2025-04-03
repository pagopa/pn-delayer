package it.pagopa.pn.delayer.middleware.dao.entity;

import lombok.Data;

@Data
public class PaperDeliveryDriverCapacitiesDispatched {
    //map Key DeliveryDriverId##Geokey##deliveryDate
    private String pk;
    private String deliveryDate;
    private String tenderId;
    private String deliveryDriverId;
    private String geoKey;
    private int capacity;
}

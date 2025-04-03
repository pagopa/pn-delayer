package it.pagopa.pn.delayer.middleware.dao.entity;

import lombok.Data;

@Data
public class PaperDeliveryDriverCapacities {
    //map key tenderId##deliveryDriverId##geoKey##activationDateFrom
    private String pk;
    private String activationDateFrom;
    private String activationDateTo;
    private String tenderId;
    private String deliveryDriverId;
    private String geoKey;
    private int capacity;
    private int peakCapacity;
    private String createdAt;
}

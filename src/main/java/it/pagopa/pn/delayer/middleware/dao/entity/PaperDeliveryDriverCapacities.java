package it.pagopa.pn.delayer.middleware.dao.entity;

import lombok.Data;

import java.time.Instant;

@Data
public class PaperDeliveryDriverCapacities {
    //map key tenderId##deliveryDriverId##geoKey##activationDateFrom
    private String pk;
    private Instant activationDateFrom;
    private Instant activationDateTo;
    private String tenderId;
    private String deliveryDriverId;
    private String geoKey;
    private int capacity;
    private int peakCapacity;
    private String createdAt;
}

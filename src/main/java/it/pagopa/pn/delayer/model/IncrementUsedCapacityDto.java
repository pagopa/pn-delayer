package it.pagopa.pn.delayer.model;

import java.time.Instant;

public record IncrementUsedCapacityDto(String unifiedDeliveryDriver,
                                       String geoKey,
                                       Integer numberOfDeliveries,
                                       Instant deliveryWeek) {}

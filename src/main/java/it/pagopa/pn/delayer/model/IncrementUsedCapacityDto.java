package it.pagopa.pn.delayer.model;

import java.time.LocalDate;

public record IncrementUsedCapacityDto(String unifiedDeliveryDriver,
                                       String geoKey,
                                       Integer numberOfDeliveries,
                                       LocalDate deliveryWeek,
                                       Integer declaredCapacity) {}

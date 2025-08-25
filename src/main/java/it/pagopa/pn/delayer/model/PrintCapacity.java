package it.pagopa.pn.delayer.model;

import java.time.LocalDate;

public record PrintCapacity(LocalDate startConfigurationTime,
                            Integer printCapacity) {}

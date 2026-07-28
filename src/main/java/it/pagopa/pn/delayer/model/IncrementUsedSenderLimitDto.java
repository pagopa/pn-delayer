package it.pagopa.pn.delayer.model;

import java.time.LocalDate;

public record IncrementUsedSenderLimitDto(String pk, Integer increment, Integer senderLimit, LocalDate shipmentDate) {}

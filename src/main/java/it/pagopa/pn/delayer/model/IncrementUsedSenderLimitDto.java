package it.pagopa.pn.delayer.model;

public record IncrementUsedSenderLimitDto(String pk, Integer increment, Integer senderLimit) {}

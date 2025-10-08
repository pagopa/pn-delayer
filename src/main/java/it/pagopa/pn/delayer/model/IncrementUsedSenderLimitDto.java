package it.pagopa.pn.delayer.model;

public record IncrementUsedSenderLimitDto(String pk, Long increment, Integer senderLimit) {}

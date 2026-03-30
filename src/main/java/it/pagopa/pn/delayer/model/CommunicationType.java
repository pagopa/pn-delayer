package it.pagopa.pn.delayer.model;

import lombok.Getter;

@Getter
public enum CommunicationType {

    INFORMAL("INFORMAL"),
    LEGAL("LEGAL");

    private final String value;

    CommunicationType(String value) {
        this.value = value;
    }
}

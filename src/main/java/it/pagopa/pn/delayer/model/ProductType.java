package it.pagopa.pn.delayer.model;

import lombok.Getter;

@Getter
public enum ProductType {

    AR("AR"),
    _890("890"),
    RS("RS"),
    RIR("RIR"),
    RIS("RIS");

    private final String value;

    ProductType(String value) {
        this.value = value;
    }
}

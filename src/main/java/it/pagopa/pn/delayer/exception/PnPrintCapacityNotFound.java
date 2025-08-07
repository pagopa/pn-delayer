package it.pagopa.pn.delayer.exception;

import java.time.LocalDate;

public class PnPrintCapacityNotFound extends RuntimeException {
    public PnPrintCapacityNotFound(LocalDate date) {
        super("No print capacity for date: " + date);
    }
}

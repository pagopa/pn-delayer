package it.pagopa.pn.delayer.exception;

import it.pagopa.pn.commons.exceptions.PnInternalException;

public class InvalidSenderLimitException extends PnInternalException {

    public InvalidSenderLimitException(String message, String errorCode) {
        super(message, errorCode);
    }
}

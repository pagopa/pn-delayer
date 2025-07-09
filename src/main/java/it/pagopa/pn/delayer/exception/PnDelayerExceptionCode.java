package it.pagopa.pn.delayer.exception;

import it.pagopa.pn.commons.exceptions.PnExceptionsCodes;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PnDelayerExceptionCode extends PnExceptionsCodes {

    public static  final String ERROR_CODE_NO_DELIVERY_DATE = "PN_DELAYER_NO_DELIVERY_DATE";
    public static  final String ERROR_CODE_INSERT_PAPER_DELIVERY_ENTITY = "PN_DELAYER_INSERT_PAPER_DELIVERY_ENTITY_ERROR";

}

package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import lombok.Data;

import java.util.List;

@Data
public class SenderLimitJobPaperDeliveries {
    List<PaperDelivery> sendToResidualStep;
    List<PaperDelivery> sendToNextStep;
}

package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SenderLimitJobPaperDeliveries {
    List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
    List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();

}

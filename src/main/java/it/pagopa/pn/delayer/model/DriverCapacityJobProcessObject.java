package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DriverCapacityJobProcessObject {
    List<PaperDelivery> toNextStep = new ArrayList<>();
    List<PaperDelivery> toNextWeek = new ArrayList<>();
    List<IncrementUsedCapacityDto> incrementUsedCapacityDtosForCap = new ArrayList<>();
}

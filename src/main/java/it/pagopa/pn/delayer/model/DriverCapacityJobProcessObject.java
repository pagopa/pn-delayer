package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DriverCapacityJobProcessObject {
    private List<PaperDelivery> toNextStep = new ArrayList<>();
    private List<PaperDelivery> toNextWeek = new ArrayList<>();
    private List<IncrementUsedCapacityDto> incrementUsedCapacityDtosForCap = new ArrayList<>();
}

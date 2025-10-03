package it.pagopa.pn.delayer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DriverCapacityJobProcessResult {
    private int sentToNextStep;
    private List<IncrementUsedCapacityDto> incrementUsedCapacityDtos = new ArrayList<>();
}

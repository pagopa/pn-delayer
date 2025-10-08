package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import lombok.Data;
import reactor.util.function.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SenderLimitJobProcessObjects {
    private List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
    private List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();
    private Map<String, Tuple2<Integer, Integer>> senderLimitMap = new HashMap<>();
    private Map<String, Integer> totalEstimateCounter = new HashMap<>();
    private List<IncrementUsedSenderLimitDto> incrementUsedSenderLimitDtoList = new ArrayList<>();
    private Map<Integer, List<String>> priorityMap;

}

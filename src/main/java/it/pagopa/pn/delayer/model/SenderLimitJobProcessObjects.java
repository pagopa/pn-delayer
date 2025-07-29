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
    List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
    List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();
    Map<String, Tuple2<Integer, Integer>> senderLimitMap = new HashMap<>();
    Map<String, Integer> totalEstimateCounter = new HashMap<>();

}

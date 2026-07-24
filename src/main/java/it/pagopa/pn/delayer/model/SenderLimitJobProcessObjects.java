package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import lombok.Data;
import java.util.*;

@Data
public class SenderLimitJobProcessObjects {
    private List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
    private List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();
    private Map<String, SenderLimitData> senderLimitMap = new HashMap<>();
    private Map<String, Integer> totalEstimateCounter = new HashMap<>();
    private List<IncrementUsedSenderLimitDto> incrementUsedSenderLimitDtoList = new ArrayList<>();
    private Map<Integer, List<PaperDeliveryPriority>> priorityMap;
    private List<PaperDeliveryCounter> delayedCounters = new ArrayList<>();
    private Map<String, Integer> delayedResidualCapacityMap = new HashMap<>();

    public record SenderLimitData(Integer weeklyEstimate, Integer calculatedLimit, Integer usedLimit) {

        public SenderLimitData {
            weeklyEstimate = Objects.requireNonNullElse(weeklyEstimate, 0);
            calculatedLimit = Objects.requireNonNullElse(calculatedLimit, 0);
            usedLimit = Objects.requireNonNullElse(usedLimit, 0);
        }

        public static SenderLimitData initial(Integer weeklyEstimate, Integer calculatedLimit) {
            return new SenderLimitData(weeklyEstimate, calculatedLimit, 0);
        }

        public int availableLimit() {
            return Math.max(calculatedLimit - usedLimit, 0);
        }

        public int availableWeeklyLimit() {
            return Math.max(weeklyEstimate - usedLimit, 0);
        }

        public SenderLimitData incrementUsedLimit(int quantity) {
            if (quantity < 0) {
                throw new IllegalArgumentException("The quantity used to increment the limit cannot be negative");
            }
            return new SenderLimitData(weeklyEstimate, calculatedLimit, usedLimit + quantity);
        }
    }
}

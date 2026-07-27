package it.pagopa.pn.delayer.model;

import java.time.LocalDate;
import java.util.Objects;

public record SenderLimitData(Integer weeklyEstimate, Integer calculatedLimit, Integer usedLimit, LocalDate date) {

    public SenderLimitData {
        weeklyEstimate = Objects.requireNonNullElse(weeklyEstimate, 0);
        calculatedLimit = Objects.requireNonNullElse(calculatedLimit, 0);
        usedLimit = Objects.requireNonNullElse(usedLimit, 0);
    }

    public static SenderLimitData initial(Integer weeklyEstimate, Integer calculatedLimit, LocalDate date) {
        return new SenderLimitData(weeklyEstimate, calculatedLimit, 0, date);
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
        return new SenderLimitData(weeklyEstimate, calculatedLimit, usedLimit + quantity, date);
    }
}

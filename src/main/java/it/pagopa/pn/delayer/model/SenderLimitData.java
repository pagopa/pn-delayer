package it.pagopa.pn.delayer.model;

import java.time.LocalDate;
import java.util.Objects;

public record SenderLimitData(Integer weeklyEstimate, Integer calculatedLimit, Integer baselineUsedLimit, Integer incrementUsedLimit, LocalDate date) {

    public SenderLimitData {
        weeklyEstimate = Objects.requireNonNullElse(weeklyEstimate, 0);
        calculatedLimit = Objects.requireNonNullElse(calculatedLimit, 0);
        baselineUsedLimit = Objects.requireNonNullElse(baselineUsedLimit, 0);
        incrementUsedLimit = Objects.requireNonNullElse(incrementUsedLimit, 0);
    }

    public static SenderLimitData initial(Integer weeklyEstimate, Integer calculatedLimit, LocalDate date) {
        return new SenderLimitData(weeklyEstimate, calculatedLimit, 0, 0, date);
    }

    public static SenderLimitData initialWithBaseline(Integer weeklyEstimate, Integer calculatedLimit, Integer baselineUsedLimit, LocalDate date) {
        return new SenderLimitData(weeklyEstimate, calculatedLimit, baselineUsedLimit, 0, date);
    }

    public int totalUsedLimit() {
        return baselineUsedLimit + incrementUsedLimit;
    }

    public int availableLimit() {
        return Math.max(calculatedLimit - totalUsedLimit(), 0);
    }

    public SenderLimitData incrementUsedLimit(int quantity) {
        if (quantity < 0) {
            throw new IllegalArgumentException("The quantity used to increment the limit cannot be negative");
        }

        return new SenderLimitData(
                weeklyEstimate,
                calculatedLimit,
                baselineUsedLimit,
                incrementUsedLimit + quantity,
                date
        );
    }
}
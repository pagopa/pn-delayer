const { DayOfWeek, TemporalAdjusters, Instant, ZoneOffset, LocalDate } = require('@js-joda/core');

function isSameISOWeek(executionDate, deliveryDate) {
  const executionLocalDate = executionDate
    .atZone(ZoneOffset.UTC)
    .toLocalDate();

  const executionWeekMonday = executionLocalDate.with(
    TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)
  );

  return executionWeekMonday.equals(deliveryDate);
}

function calculateDeliveryDate() {
    const now = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();
    const monday = now.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
    return monday;
}

module.exports = { isSameISOWeek, calculateDeliveryDate };

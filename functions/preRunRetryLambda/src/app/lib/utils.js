const { DayOfWeek, TemporalAdjusters, Instant, ZoneOffset, LocalDate } = require('@js-joda/core');

function toInstant(value) {
  if (value instanceof Instant) return value;

  // Date JS o stringa tipo "Wed Jan 21 2026 14:06:25 GMT+0000 ..."
  if (value instanceof Date || typeof value === 'string') {
    return Instant.ofEpochMilli(new Date(value).getTime());
  }

  // epoch millis
  if (typeof value === 'number') {
    return Instant.ofEpochMilli(value);
  }

  throw new Error('Unsupported executionDate type');
}

function isSameISOWeek(executionDate, deliveryDate) {
  const executionInstant = toInstant(executionDate);

  const executionLocalDate = executionInstant
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

function normalizeToLocalDate(deliveryDate) {
  if (deliveryDate == null || deliveryDate === "") {
    return calculateDeliveryDate();
  }

  if (deliveryDate instanceof Date) {
    return Instant.ofEpochMilli(deliveryDate.getTime())
      .atZone(ZoneOffset.UTC)
      .toLocalDate();
  }

  // stringa YYYY-MM-DD
  if (typeof deliveryDate === "string") {
    return LocalDate.parse(deliveryDate);
  }

  // già LocalDate
  return deliveryDate;
}

module.exports = { isSameISOWeek, calculateDeliveryDate, normalizeToLocalDate };

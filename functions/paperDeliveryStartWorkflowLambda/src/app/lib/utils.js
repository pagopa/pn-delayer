const { LocalDate, DayOfWeek, TemporalAdjusters } = require('@js-joda/core');

function getDeliveryWeek() {
  const dayOfWeek = parseInt(process.env.DELIVERY_DATE_DAY_OF_WEEK, 10) || 1;
  return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
}

module.exports = { getDeliveryWeek };
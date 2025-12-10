"use strict";
const { DayOfWeek, TemporalAdjusters, Instant, ZoneOffset, LocalDate } = require('@js-joda/core');

exports.handleEvent = async (event = {}) => {
  console.log("Event received:", JSON.stringify(event));

  const deliveryWeek = event.deliveryWeek ?? undefined;

  if (deliveryWeek) {
    return deliveryWeek.toString();
  }

  const now = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();

  const monday = now.with(
    TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)
  );

  return monday.toString();
};
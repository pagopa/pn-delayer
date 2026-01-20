const { expect } = require("chai");
const {
  DayOfWeek,
  Instant,
  LocalDate
} = require("@js-joda/core");

const {
  isSameISOWeek,
  calculateDeliveryDate
} = require("../app/lib/utils");

describe("utils", () => {
  describe("isSameISOWeek", () => {
    it("should return true for dates in the same ISO week", () => {
      // Monday 2025-01-13
      const executionDate = Instant.parse("2025-01-13T10:00:00Z");

      // Sunday of the same ISO week → Monday is still 2025-01-13
      const deliveryDate = LocalDate.parse("2025-01-13");

      expect(isSameISOWeek(executionDate, deliveryDate)).to.equal(true);
    });

    it("should return false for dates in different ISO weeks", () => {
      // Sunday night (previous ISO week)
      const executionDate = Instant.parse("2025-01-12T23:00:00Z");

      // Monday of next ISO week
      const deliveryDate = LocalDate.parse("2025-01-13");

      expect(isSameISOWeek(executionDate, deliveryDate)).to.equal(false);
    });
  });

  describe("calculateDeliveryDate", () => {
    it("should return the Monday of the current ISO week", () => {
      const deliveryDate = calculateDeliveryDate();

      expect(deliveryDate.dayOfWeek()).to.equal(DayOfWeek.MONDAY);
    });
  });
});

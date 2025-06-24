const { getDeliveryWeek } = require('../../app/lib/utils');
const { expect } = require("chai");
const { LocalDate, DayOfWeek, TemporalAdjusters } = require('@js-joda/core');

describe('getDeliveryWeek', () => {

  it('return monday of current week if env is not set', () => {
    delete process.env.DELIVERY_DATE_DAY_OF_WEEK;
    const expected = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).toString();
    expect(getDeliveryWeek()).to.equal(expected);
  });

  it('return monday of current week if env is set', () => {
    process.env.DELIVERY_DATE_DAY_OF_WEEK = '1'; // 1 = Monday
    const expected = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).toString();
    expect(getDeliveryWeek()).to.equal(expected);
  });

  it('return monday of current week if env is set', () => {
    process.env.DELIVERY_DATE_DAY_OF_WEEK = '5'; // 5 = Friday
    const expected = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY)).toString();
    expect(getDeliveryWeek()).to.equal(expected);
  });

  it('handle non numerical value in env', () => {
    process.env.DELIVERY_DATE_DAY_OF_WEEK = 'abc';
    const expected = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).toString();
    expect(getDeliveryWeek()).to.equal(expected);
  });
});
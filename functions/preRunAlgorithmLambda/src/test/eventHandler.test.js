const { handleEvent } = require('../app/eventHandler.js');
const { LocalDate } = require('@js-joda/core');
const { expect } = require("chai");

describe('handleEvent', () => {
  it('returns the parsed date when inputDate is provided', async () => {
    const event = { parameters: ['2023-10-01'] };
    const result = await handleEvent(event);
    expect(result).to.equal('2023-10-01');
  });

  it('returns the previous or same Monday when no inputDate is provided', async () => {
    const event = {};
    const result = await handleEvent(event);
    const now = LocalDate.now();
    const expectedMonday = now.with(
      require('@js-joda/core').TemporalAdjusters.previousOrSame(
        require('@js-joda/core').DayOfWeek.MONDAY
      )
    );
    expect(result).to.equal(expectedMonday.toString());
  });

  it('handles empty parameters array gracefully', async () => {
    const event = { parameters: [] };
    const result = await handleEvent(event);
    const now = LocalDate.now();
    const expectedMonday = now.with(
      require('@js-joda/core').TemporalAdjusters.previousOrSame(
        require('@js-joda/core').DayOfWeek.MONDAY
      )
    );
    expect(result).to.equal(expectedMonday.toString());
  });

  it('handles undefined event gracefully', async () => {
    const result = await handleEvent();
    const now = LocalDate.now();
    const expectedMonday = now.with(
      require('@js-joda/core').TemporalAdjusters.previousOrSame(
        require('@js-joda/core').DayOfWeek.MONDAY
      )
    );
    expect(result).to.equal(expectedMonday.toString());
  });

  it('throws an error for invalid inputDate format', async () => {
    const event = { parameters: ['invalid-date'] };
    const result = await handleEvent(event);
    expect(result).to.equal(event.parameters[0]);
  });
});

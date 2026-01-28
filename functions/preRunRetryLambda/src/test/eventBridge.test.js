const { expect } = require("chai");
const { mockClient } = require("aws-sdk-client-mock");
const {
  SchedulerClient,
  GetScheduleCommand
} = require("@aws-sdk/client-scheduler");
  const { LocalDate } = require("@js-joda/core");

const { getActiveScheduler } = require("../app/lib/eventBridge");

const schedulerMock = mockClient(SchedulerClient);

describe("eventBridge", () => {
  const deliveryDate = LocalDate.parse("2025-01-20");

  beforeEach(() => {
    schedulerMock.reset();
    process.env.DELAYER_TO_PAPER_CHANNEL_FIRST_SCHEDULER = "scheduler-1";
    process.env.DELAYER_TO_PAPER_CHANNEL_SECOND_SCHEDULER = "scheduler-2";
  });

  it("should return active scheduler when deliveryDate is within schedule range", async () => {
    schedulerMock.on(GetScheduleCommand, { Name: "scheduler-1" }).resolves({
      Name: "scheduler-1",
      ScheduleExpression: "cron(0 2 ? * MON *)",
      StartDate: new Date("2025-01-01T00:00:00Z"),
      EndDate: new Date("2025-12-31T23:59:59Z"),
    });

    const scheduler = await getActiveScheduler(deliveryDate);

    expect(scheduler).to.deep.equal({
      name: "scheduler-1",
      scheduleExpression: "cron(0 2 ? * MON *)",
      endDate: "2025-12-31T23:59:59.000Z"
    });
  });

  it("should skip scheduler if deliveryDate is before StartDate", async () => {
    schedulerMock.on(GetScheduleCommand).resolves({
      Name: "scheduler-1",
      ScheduleExpression: "cron(0 2 ? * MON *)",
      StartDate: new Date("2025-02-01T00:00:00Z"),
    });

    const scheduler = await getActiveScheduler(deliveryDate);

    expect(scheduler).to.equal(null);
  });

  it("should return null when no schedulers are active", async () => {
    schedulerMock.on(GetScheduleCommand).resolves({});

    const scheduler = await getActiveScheduler(deliveryDate);

    expect(scheduler).to.equal(null);
  });
});

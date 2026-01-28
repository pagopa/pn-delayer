const {
  SchedulerClient,
  GetScheduleCommand
} = require("@aws-sdk/client-scheduler");
const { Instant, ZoneId } = require("@js-joda/core");

const schedulerClient = new SchedulerClient({});

async function getActiveScheduler(currentDate) {
  const schedulerNames = [
    process.env.DELAYER_TO_PAPER_CHANNEL_FIRST_SCHEDULER,
    process.env.DELAYER_TO_PAPER_CHANNEL_SECOND_SCHEDULER
  ].filter(Boolean);

  for (const name of schedulerNames) {
    const response = await schedulerClient.send(
      new GetScheduleCommand({ Name: name })
    );

    const startDate = response.StartDate
      ? Instant.parse(response.StartDate.toISOString())
          .atZone(ZoneId.UTC)
          .toLocalDate()
      : null;
    const endDate = response.EndDate
      ? Instant.parse(response.EndDate.toISOString())
          .atZone(ZoneId.UTC)
          .toLocalDate()
      : null;
    if (!startDate) continue;

    const isAfterStart = !currentDate.isBefore(startDate);
    const isBeforeEnd = !endDate || currentDate.isBefore(endDate);
    console.log(isAfterStart);
    console.log(isBeforeEnd);
    if (isAfterStart && isBeforeEnd) {
      return {
        name: response.Name,
        scheduleExpression: response.ScheduleExpression,
        endDate: response.EndDate
           ? response.EndDate.toISOString()
           : null
      };
    }
  }

  return null;
}

module.exports = { getActiveScheduler };

const {
  SchedulerClient,
  GetScheduleCommand
} = require("@aws-sdk/client-scheduler");

const schedulerClient = new SchedulerClient({});

async function getActiveScheduler(deliveryDate) {
  const schedulerNames = [
    process.env.DELAYER_TO_PAPER_CHANNEL_FIRST_SCHEDULER,
    process.env.DELAYER_TO_PAPER_CHANNEL_SECOND_SCHEDULER
  ].filter(Boolean);

  for (const name of schedulerNames) {
    const response = await schedulerClient.send(
      new GetScheduleCommand({ Name: name })
    );

    const startDate = response.StartDate
      ? new Date(response.StartDate)
      : null;

    const endDate = response.EndDate
      ? new Date(response.EndDate)
      : null;

    if (!startDate) continue;

    const isAfterStart = deliveryDate >= startDate;
    const isBeforeEnd = !endDate || deliveryDate <= endDate;

    if (isAfterStart && isBeforeEnd) {
      return {
        name: response.Name,
        scheduleExpression: response.ScheduleExpression
      };
    }
  }

  return null;
}

module.exports = { getActiveScheduler };

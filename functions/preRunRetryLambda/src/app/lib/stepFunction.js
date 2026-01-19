const { SFNClient, ListExecutionsCommand } = require("@aws-sdk/client-sfn");
const { isSameISOWeek } = require("./utils");

const sfnClient = new SFNClient({});
const stateMachineName = process.env.RETRY_ALGORITHM_STATE_MACHINE;

/**
    * Check if there is an execution for the given state machine ARN
    * in the same ISO week as the provided delivery date.
    *
    * @param {Date} deliveryDate - The delivery date to compare against.
    * @returns {Promise<boolean>} - Returns true if no execution exists in the same week, false otherwise.
    */
async function executionWithDeliveryDateExists(deliveryDate) {
  const response = await sfnClient.send(
    new ListExecutionsCommand({
      stateMachineName,
      maxResults: 2,
    })
  );

  if (!response.executions || response.executions.length === 0) {
    console.log("No execution found for:", stateMachineName);
    return true;
  }

  const completedExecutions = response.executions.filter(
      (e) => e.status !== "RUNNING"
    );

  const lastCompletedExecution = completedExecutions.reduce(
    (latest, current) =>
      current.startDate > latest.startDate ? current : latest
  );

  const lastCompletedExecutionStartDate = lastCompletedExecution.startDate;

  if (isSameISOWeek(lastCompletedExecutionStartDate, deliveryDate)) {
    console.log("Execution found for the same week:", stateMachineName);
    return false;
  }

  console.log("No execution found for the same week:", stateMachineName);
  return true;
}

module.exports = { executionWithDeliveryDateExists };

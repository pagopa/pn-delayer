const { SFNClient, ListExecutionsCommand } = require("@aws-sdk/client-sfn");
const { isSameISOWeek } = require("./utils");

const sfnClient = new SFNClient({});
const stateMachineArn = process.env.RETRY_ALGORITHM_STATE_MACHINE_ARN;

/**
    * Check if there is an execution for the given state machine ARN
    * in the same ISO week as the provided delivery date.
    *
    * @param {LocalDate} deliveryDate - The delivery date to compare against.
    * @returns {Promise<boolean>} - Returns true if no execution exists in the same week, false otherwise.
    */
async function executionWithDeliveryDateExists(deliveryDate, currentExecutionArn) {
    const response = await sfnClient.send(
        new ListExecutionsCommand({
          stateMachineArn,
          maxResults: 10,
        })
    );

    if (!response.executions || response.executions.length === 0) {
        console.log("No execution found for:", stateMachineArn);
        return true;
    }

    //exclude current execution
    const otherExecutions = response.executions.filter(
        (e) => e.executionArn !== currentExecutionArn
    );

    const otherRunning = otherExecutions.find(
      (e) => e.status === "RUNNING"
    );

    if (otherRunning) {
        console.log(
          "Another execution already running:",
          otherRunning.executionArn
        );
        return false;
    }

    const completedExecutions = otherExecutions.filter(
        (e) => e.status !== "RUNNING"
      );

    if (completedExecutions.length === 0) {
      console.log("No completed executions found for:", stateMachineArn);
      return true;
    }

    const lastCompletedExecution = completedExecutions.reduce(
      (latest, current) =>
        current.startDate > latest.startDate ? current : latest
    );

    const lastCompletedExecutionStartDate = lastCompletedExecution.startDate;

    if (isSameISOWeek(lastCompletedExecutionStartDate, deliveryDate)) {
        console.log("Execution found for the same week:", stateMachineArn);
        return false;
    }

    console.log("No execution found for the same week:", stateMachineArn);
    return true;
}

module.exports = { executionWithDeliveryDateExists };

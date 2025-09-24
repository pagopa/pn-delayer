"use strict";
const { SFNClient, DescribeExecutionCommand } = require("@aws-sdk/client-sfn");

/**
 * Returns the status of a specific execution of the step function.
 * @param {Array<string>} params [executionArn]
 */
async function getStatusExecution(params = []) {
  const [executionArn] = params;
  if (!executionArn) {
    throw new Error("Parameter must be [executionArn]");
  }

  const sfnClient = new SFNClient({});
  const command = new DescribeExecutionCommand({ executionArn });
  const response = await sfnClient.send(command);

  const result = {
    executionArn: executionArn,
    status: response.status,
    startDate: response.startDate,
    stopDate: response.stopDate,
  };

  if (response.status === "FAILED") {
    result.error = response.error || null;
    result.cause = response.cause || null;
  }

  return result;
}

module.exports = { getStatusExecution };

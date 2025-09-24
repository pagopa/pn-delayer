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
        executionId: executionArn,
        status: response.status,
        startDate: response.startDate,
        endDate: response.stopDate,
    };

    if (response.status === "FAILED" && response.cause) {
        result.error = response.cause;
    }

    return result;
}

module.exports = { getStatusExecution };
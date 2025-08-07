"use strict";

const { SFNClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});


/**
 * DELAYER_TO_PAPER_CHANNEL operation: run delayerToPaperChannel Step Function
 * @param {Array<string>} params[deliveryDateDayOfWeek]
 */
async function runDelayerToPaperChannelWorkflow(params) {
    const { DELAYERTOPAPERCHANNEL_SFN_ARN } = process.env;
    const [deliveryDateDayOfWeek] = params;
    if (!DELAYERTOPAPERCHANNEL_SFN_ARN) throw new Error("Missing environment variable DELAYERTOPAPERCHANNEL_SFN_ARN");

    let INPUT = {
        PAPERDELIVERYCOUNTER_TABLENAME: "pn-PaperDeliveryCounters",
        PN_DELAYER_DELIVERYDATEDAYOFWEEK: deliveryDateDayOfWeek || "1"
    };


    const cmd = new StartExecutionCommand({
        stateMachineArn: DELAYERTOPAPERCHANNEL_SFN_ARN,
        input: JSON.stringify(INPUT),
    });

    const { executionArn, startDate } = await sfnClient.send(cmd);
    return { message: "Step Function started", executionArn, startDate };
}

module.exports = { runDelayerToPaperChannelWorkflow };
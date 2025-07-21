"use strict";

const { SFNClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});
const STATIC_INPUT = {
    PaperDeliveryTableName: "pn-DelayerPaperDelivery",
    DeliveryDriverCapacityTableName: "pn-PaperDeliveryDriverCapacities",
    DeliveryDriverUsedCapacityTableName: "pn-PaperDeliveryDriverUsedCapacities",
    EstimateSendersTableName: "pn-PaperDeliveryEstimateSenders",
    SenderUsedLimitTableName: "pn-PaperDeliveriesSenderUsedLimit",
    PrintCapacityCounterTableName: "pn-PaperDeliveriesPrintCapacityCounter",
    CounterTableName: "pn-PaperDeliveriesCounter",
};

/**

 RUN_ALGORITHM operation – avvia la Step Function configurata.

 Nessun parametro d'ingresso.
 */
async function runAlgorithm() {
    const { SFN_ARN } = process.env;
    if (!SFN_ARN) throw new Error("Missing environment variable SFN_ARN");

    const cmd = new StartExecutionCommand({
        stateMachineArn: SFN_ARN,
        input: JSON.stringify(STATIC_INPUT),
    });

    const { executionArn, startDate } = await sfnClient.send(cmd);
    return { message: "Step Function started", executionArn, startDate };
}

module.exports = { runAlgorithm };
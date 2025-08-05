"use strict";

const { SFNClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});
const STATIC_INPUT = {
    PAPERDELIVERY_TABLENAME: "pn-DelayerPaperDelivery",
    PAPERDELIVERYDRIVERCAPACITIES_TABLENAME: "pn-PaperDeliveryDriverCapacities",
    PAPERDELIVERYDRIVERUSEDCAPACITIES_TABLENAME: "pn-PaperDeliveryDriverUsedCapacities",
    PAPERDELIVERYSENDERLIMIT_TABLENAME: "pn-PaperDeliverySenderLimit",
    PAPERDELIVERYUSEDSENDERLIMIT_TABLENAME: "pn-PaperDeliveryUsedSenderLimit",
    PAPERDELIVERYPRINTCAPACITY_TABLENAME: "pn-PaperDeliveryPrintCapacity",
    PAPERDELIVERYCOUNTER_TABLENAME: "pn-PaperDeliveryCounters",
    PN_DELAYER_DELIVERYDATEDAYOFWEEK: "1"
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
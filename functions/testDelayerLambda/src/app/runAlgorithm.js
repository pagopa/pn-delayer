"use strict";

const { SFNClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});

/**
 *RUN_ALGORITHM operation – avvia la Step Function configurata.
 * @param {Array<string>} params[printCapacity, deliveryDateDayOfWeek]
 */
async function runAlgorithm(params) {
    const { SFN_ARN } = process.env;
    if (!SFN_ARN) throw new Error("Missing environment variable SFN_ARN");
    let [printCapacity, deliveryDateDayOfWeek] = params;

    if (!printCapacity) {
        printCapacity = "180000"
    }
    const printCapacityValue = `1970-01-01;${printCapacity}`;

    let INPUT = {
        PAPERDELIVERY_TABLENAME: "pn-DelayerPaperDelivery",
        PAPERDELIVERYDRIVERCAPACITIES_TABLENAME: "pn-PaperDeliveryDriverCapacities",
        PAPERDELIVERYDRIVERUSEDCAPACITIES_TABLENAME: "pn-PaperDeliveryDriverUsedCapacities",
        PAPERDELIVERYSENDERLIMIT_TABLENAME: "pn-PaperDeliverySenderLimit",
        PAPERDELIVERYUSEDSENDERLIMIT_TABLENAME: "pn-PaperDeliveryUsedSenderLimit",
        PAPERDELIVERYPRINTCAPACITY_TABLENAME: "pn-PaperDeliveryPrintCapacity",
        PAPERDELIVERYCOUNTER_TABLENAME: "pn-PaperDeliveryCounters",
        PN_DELAYER_DELIVERYDATEDAYOFWEEK: deliveryDateDayOfWeek || "1",
        PN_DELAYER_PRINTCAPACITY: printCapacityValue
    };

    const cmd = new StartExecutionCommand({
        stateMachineArn: SFN_ARN,
        input: JSON.stringify(INPUT),
    });

    const { executionArn, startDate } = await sfnClient.send(cmd);
    return { message: "Step Function started", executionArn, startDate };
}

module.exports = { runAlgorithm };
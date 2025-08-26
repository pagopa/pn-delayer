"use strict";

const { SFNClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});

/**
 *RUN_ALGORITHM operation – avvia la Step Function configurata.
 * @param {Array<string>} params[paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName,
 *         senderLimitTableName, senderUsedLimitTableName, printCapacityTableName, countersTableName, printCapacity,
 *         deliveryDateDayOfWeek]
 */
async function runAlgorithm(params) {
    const { SFN_ARN } = process.env;
    if (!SFN_ARN) throw new Error("Missing environment variable SFN_ARN");
    let [paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName,
        senderLimitTableName, senderUsedLimitTableName, printCapacityTableName, countersTableName, printCapacity,
        deliveryDateDayOfWeek] = params;

    if (!printCapacity) {
        printCapacity = "180000"
    }
    const printCapacityValue = `1970-01-01;${printCapacity}`;

    let INPUT = {
        PAPERDELIVERY_TABLENAME: paperDeliveryTableName, //"pn-DelayerPaperDelivery",
        PAPERDELIVERYDRIVERCAPACITIES_TABLENAME: deliveryDriverCapacitiesTableName, //"pn-PaperDeliveryDriverCapacities",
        PAPERDELIVERYDRIVERUSEDCAPACITIES_TABLENAME: deliveryDriverUsedCapacitiesTableName, //"pn-PaperDeliveryDriverUsedCapacities",
        PAPERDELIVERYSENDERLIMIT_TABLENAME: senderLimitTableName, //"pn-PaperDeliverySenderLimit",
        PAPERDELIVERYUSEDSENDERLIMIT_TABLENAME: senderUsedLimitTableName, //"pn-PaperDeliveryUsedSenderLimit",
        PAPERDELIVERYPRINTCAPACITY_TABLENAME: printCapacityTableName, //"pn-PaperDeliveryPrintCapacity",
        PAPERDELIVERYCOUNTER_TABLENAME: countersTableName, //"pn-PaperDeliveryCounters",
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
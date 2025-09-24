"use strict";

const { SFNClient, StartExecutionCommand, ListExecutionsCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});

async function getLastExecution(stateMachineArn) {
    const resp = await sfnClient.send(
        new ListExecutionsCommand({
            stateMachineArn,
            maxResults: 1,
            reverseOrder: true
        })
    );
    return resp.executions?.[0] || null;
}

/**
 *RUN_ALGORITHM operation – launches the configured Step Function only if there are no active runs.
 * @param {Array<string>} params[paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName,
 *         senderLimitTableName, senderUsedLimitTableName, countersTableName, printCapacity]
 */
async function runAlgorithm(params) {
    const { SFN_ARN } = process.env;
    if (!SFN_ARN) throw new Error("Missing environment variable SFN_ARN");
    let [paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName,
        senderLimitTableName, senderUsedLimitTableName, countersTableName, printCapacity] = params;

    if (!printCapacity) {
        printCapacity = "180000"
    }
    const printCapacityValue = `1970-01-01;${printCapacity}`;

    if (!paperDeliveryTableName || !deliveryDriverCapacitiesTableName || !deliveryDriverUsedCapacitiesTableName ||
        !senderLimitTableName || !senderUsedLimitTableName || !countersTableName) {
        throw new Error("Required parameters must be [paperDeliveryTableName, deliveryDriverCapacitiesTableName, " +
            "deliveryDriverUsedCapacitiesTableName, senderLimitTableName, senderUsedLimitTableName, countersTableName]");
    }

    const lastExecution = await getLastExecution(SFN_ARN);
    if (lastExecution && (lastExecution.status === "RUNNING" || lastExecution.status === "PENDING_REDRIVE")) {
        return {
            message: "There is already an active execution of the Step Function",
            executionArn: lastExecution.executionArn,
            status: lastExecution.status
        };
    }

    let INPUT = {
        PAPERDELIVERY_TABLENAME: paperDeliveryTableName, //"pn-DelayerPaperDelivery",
        PAPERDELIVERYDRIVERCAPACITIES_TABLENAME: deliveryDriverCapacitiesTableName, //"pn-PaperDeliveryDriverCapacities",
        PAPERDELIVERYDRIVERUSEDCAPACITIES_TABLENAME: deliveryDriverUsedCapacitiesTableName, //"pn-PaperDeliveryDriverUsedCapacities",
        PAPERDELIVERYSENDERLIMIT_TABLENAME: senderLimitTableName, //"pn-PaperDeliverySenderLimit",
        PAPERDELIVERYUSEDSENDERLIMIT_TABLENAME: senderUsedLimitTableName, //"pn-PaperDeliveryUsedSenderLimit",
        PAPERDELIVERYCOUNTER_TABLENAME: countersTableName, //"pn-PaperDeliveryCounters",
        PN_DELAYER_DELIVERYDATEDAYOFWEEK: "1",
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
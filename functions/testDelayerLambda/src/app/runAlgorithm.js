"use strict";

const { LocalDate, DayOfWeek, TemporalAdjusters } = require("@js-joda/core");
const { SFNClient, StartExecutionCommand, ListExecutionsCommand } = require("@aws-sdk/client-sfn");

const sfnClient = new SFNClient({});

async function getLastExecution(stateMachineArn) {
    const resp = await sfnClient.send(
        new ListExecutionsCommand({
            stateMachineArn,
            maxResults: 1,
            reverseOrder: true,
        })
    );
    return resp.executions?.[0] || null;
}

/**
 *RUN_ALGORITHM operation – launches the configured Step Function only if there are no active runs.
 * @param {Array<string>} params[paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName,
 *         senderLimitTableName, senderUsedLimitTableName, countersTableName, opt <{"printCapacity":"", "deliveryWeek":""}>]
 */
async function runAlgorithm(params) {
  try {
    const { SFN_ARN, DELAYERTOPAPERCHANNELFIRSTSCHEDULERCRON, DELAYERTOPAPERCHANNELSECONDSCHEDULERCRON,
    DELAYERTOPAPERCHANNELFIRSTSCHEDULERSTARTDATE, DELAYERTOPAPERCHANNELSECONDSCHEDULERSTARTDATE} = process.env;
    if (!SFN_ARN || !DELAYERTOPAPERCHANNELFIRSTSCHEDULERCRON || !DELAYERTOPAPERCHANNELSECONDSCHEDULERCRON ||
    !DELAYERTOPAPERCHANNELFIRSTSCHEDULERSTARTDATE || !DELAYERTOPAPERCHANNELSECONDSCHEDULERSTARTDATE) {
      return resp(500, { error: "Missing required environment variables" });
    }
    let [paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName,
        senderLimitTableName, senderUsedLimitTableName, countersTableName, printCapacity, deliveryWeek] = params;

     if (!paperDeliveryTableName || !deliveryDriverCapacitiesTableName || !deliveryDriverUsedCapacitiesTableName ||
            !senderLimitTableName || !senderUsedLimitTableName || !countersTableName || !printCapacity) {
          return resp(400, {
            error:
              "Required parameters must be [paperDeliveryTableName, deliveryDriverCapacitiesTableName, deliveryDriverUsedCapacitiesTableName, senderLimitTableName, senderUsedLimitTableName, countersTableName, printCapacity]",
          });
    }

    const printCapacityValue = `1970-01-01;${printCapacity}`;

    if(!deliveryWeek) {
      deliveryWeek = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(1))).toString();
    }

    const lastExecution = await getLastExecution(SFN_ARN);
    if (lastExecution && (lastExecution.status === "RUNNING" || lastExecution.status === "PENDING_REDRIVE") ) {
      return resp(429, {
        message: "There is already an active execution of the Step Function",
        executionArn: lastExecution.executionArn,
        status: lastExecution.status,
      });
    }

    let INPUT = {
        PAPERDELIVERY_TABLENAME: paperDeliveryTableName, //"pn-DelayerPaperDelivery",
        PAPERDELIVERYDRIVERCAPACITIES_TABLENAME: deliveryDriverCapacitiesTableName, //"pn-PaperDeliveryDriverCapacities",
        PAPERDELIVERYDRIVERUSEDCAPACITIES_TABLENAME: deliveryDriverUsedCapacitiesTableName, //"pn-PaperDeliveryDriverUsedCapacities",
        PAPERDELIVERYSENDERLIMIT_TABLENAME: senderLimitTableName, //"pn-PaperDeliverySenderLimit",
        PAPERDELIVERYUSEDSENDERLIMIT_TABLENAME: senderUsedLimitTableName, //"pn-PaperDeliveryUsedSenderLimit",
        PAPERDELIVERYCOUNTER_TABLENAME: countersTableName, //"pn-PaperDeliveryCounters",
        PN_DELAYER_DELIVERYDATEDAYOFWEEK: "1",
        PN_DELAYER_PRINTCAPACITY: printCapacityValue,
        PN_DELAYER_DELIVERYWEEK: deliveryWeek,
        PN_DELAYER_DELAYERTOPAPERCHANNELFIRSTSCHEDULERCRON: DELAYERTOPAPERCHANNELFIRSTSCHEDULERCRON,
        PN_DELAYER_DELAYERTOPAPERCHANNELSECONDSCHEDULERCRON: DELAYERTOPAPERCHANNELSECONDSCHEDULERCRON,
        PN_DELAYER_DELAYERTOPAPERCHANNELFIRSTSCHEDULERSTARTDATE: DELAYERTOPAPERCHANNELFIRSTSCHEDULERSTARTDATE,
        PN_DELAYER_DELAYERTOPAPERCHANNELSECONDSCHEDULERSTARTDATE: DELAYERTOPAPERCHANNELSECONDSCHEDULERSTARTDATE
    };

    const cmd = new StartExecutionCommand({
        stateMachineArn: SFN_ARN,
        input: JSON.stringify(INPUT),
    });

    const { executionArn, startDate } = await sfnClient.send(cmd);
    return resp(200, { message: "Step Function started", executionArn, startDate });
  } catch (err) {
    console.error(err);
    return resp(500, { error: "Unexpected error", details: String(err?.message || err) });
  }
}

// helper per risposte API Gateway (lambda proxy)
function resp(statusCode, bodyObj) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(bodyObj),
  };
}

module.exports = { runAlgorithm };
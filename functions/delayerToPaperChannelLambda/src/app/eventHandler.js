const dynamo = require("./lib/dynamo");
const utils = require("./lib/utils");
const { DayOfWeek, TemporalAdjusters, Instant, ZoneOffset } = require('@js-joda/core');

exports.handleEvent = async (event) => {
    console.log("Event received:", JSON.stringify(event));

    if (!event.processType) {
        throw new Error("processType is required in the event");
    }
    const paperDeliveryTableName = event.paperDeliveryTableName;
    const dayOfWeek = parseInt(process.env.PN_DELAYER_DELIVERYDATEDAYOFWEEK, 10) || 1;
    const instant = Instant.parse(event.executionDate);
    const zone = ZoneOffset.UTC;
    const deliveryWeek = instant.atZone(zone).toLocalDate()
        .with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)))
        .toString();
    const numberOfDailyExecution = event.fixed.dailyExecutions;
    const numberOfShipmentsPerExecution = Math.ceil(event.fixed.dailyPrintCapacity / numberOfDailyExecution);
    const today = new Date().toISOString();
    const counter = parseInt(event.fixed.dailyExecutionCounter, 10) || 0;
    const maxExecutions = parseInt(numberOfDailyExecution, 10) || 0;
    let lastExecution = false;

    if (counter === maxExecutions - 1) {
        console.warn(`Last execution for day ${today} -> numberOfDailyExecution: ${numberOfDailyExecution}, currentExecutionNumber: ${event.fixed.dailyExecutionCounter}`);
        lastExecution = true;
    }
    switch (event.processType) {
        case "SEND_TO_PHASE_2": {
            const toSendToNextStep = numberOfShipmentsPerExecution - event.variable.sendToNextStepCounter;
            const weeklyResidual = event.fixed.weeklyPrintCapacity - event.fixed.sentToPhaseTwo;
            if (toSendToNextStep > 0 && !event.variable.stopSendToPhaseTwo && weeklyResidual > 0) {
                console.log(`To send to phase 2: ${toSendToNextStep}`);
                return sendToPhase2(paperDeliveryTableName, deliveryWeek, event.variable, toSendToNextStep, lastExecution);
            }
            console.log("No shipments to send to next step - weeklyResidual:", weeklyResidual, "sentToPhaseTwo:", event.fixed.sentToPhaseTwo);
            return {
                    lastEvaluatedKeyPhase2: null,
                    sendToNextStepCounter: parseInt(event.variable.sendToNextStepCounter),
                    lastExecution: lastExecution,
            };
        }
        case "SEND_TO_NEXT_WEEK": {
            const exceed = event.fixed.numberOfShipments - event.fixed.weeklyPrintCapacity;
            const toSendToNextWeek = exceed - event.fixed.sentToNextWeek - event.variable.sendToNextWeekCounter;
            if (toSendToNextWeek > 0) {
                console.log(`To send to next week: ${toSendToNextWeek}`);
                return sendToNextWeek(paperDeliveryTableName, deliveryWeek, event.variable, toSendToNextWeek, lastExecution);
            }
            console.log("No shipments to send to next week");
            return {
                    lastEvaluatedKeyNextWeek: null,
                    sendToNextWeekCounter: parseInt(event.variable.sendToNextWeekCounter),
                    lastExecution: lastExecution,
            };
        }
        default:
            throw new Error("Invalid processType. Use: SEND_TO_PHASE_2 or SEND_TO_NEXT_WEEK");
    }
};

async function sendToPhase2(paperDeliveryTableName, deliveryWeek, variable, toSendToNextStep, lastExecution) {
    return retrieveAndProcessItems(
            paperDeliveryTableName,
            deliveryWeek,
            variable.lastEvaluatedKeyPhase2,
            variable.sendToNextStepCounter,
            toSendToNextStep,
            0,
            "SENT_TO_PREPARE_PHASE_2",
            true
        )
        .then(result => {
            return {
                    lastEvaluatedKeyPhase2: remapLastEvaluatedKey(result.lastEvaluatedKey),
                    sendToNextStepCounter: parseInt(result.dailyCounter),
                    lastExecution: lastExecution,
            };
    });
}

async function sendToNextWeek(paperDeliveryTableName, deliveryWeek, variable, toSendToNextWeek, lastExecution) {
    return retrieveAndProcessItems(
            paperDeliveryTableName,
            deliveryWeek,
            variable.lastEvaluatedKeyNextWeek,
            variable.sendToNextWeekCounter,
            toSendToNextWeek,
            0,
            "EVALUATE_SENDER_LIMIT",
            false
         )
        .then(result => {
            return {
                    lastEvaluatedKeyNextWeek: remapLastEvaluatedKeyForNextWeek(result.lastEvaluatedKey, result.toHandle, result.dailyCounter),
                    sendToNextWeekCounter: parseInt(result.dailyCounter),
                    lastExecution: lastExecution,
            };
    });
}

async function retrieveAndProcessItems(paperDeliveryTableName, deliveryWeek, lastEvaluatedKey, dailyCounter, toHandle, executionCounter, step, scanIndexForward) {
    const limit = Math.min(toHandle, parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10));
    const response = await dynamo.retrieveItems(paperDeliveryTableName, deliveryWeek, lastEvaluatedKey, limit, scanIndexForward);

    if (!response.Items || response.Items.length === 0) {
        console.log(`No more items to send to step ${step}`);
        return {
            lastEvaluatedKey: null,
            dailyCounter: dailyCounter
        };
    }

    await processItems(paperDeliveryTableName, deliveryWeek, response.Items, step);

    const itemsProcessed = response.Items.length;
    dailyCounter += itemsProcessed;
    toHandle -= itemsProcessed;
    executionCounter += itemsProcessed;
    console.log(`Items processed so far for step ${step}: ${dailyCounter}, items still to handle: ${toHandle}, executionCounter: ${executionCounter}`);

     if (
        toHandle > 0 &&
        response.LastEvaluatedKey &&
        Object.keys(response.LastEvaluatedKey).length > 0 &&
        executionCounter < 5000
      ) {
        return retrieveAndProcessItems(
            paperDeliveryTableName,
            deliveryWeek,
            remapLastEvaluatedKey(response.LastEvaluatedKey),
            dailyCounter,
            toHandle,
            executionCounter,
            step,
            scanIndexForward
        );
    }

    return {
        lastEvaluatedKey: response.LastEvaluatedKey,
        dailyCounter: dailyCounter,
        toHandle: toHandle
    };
}

function remapLastEvaluatedKey(lastEvaluatedKey){
    if(lastEvaluatedKey){
        return {
        pk: { S: lastEvaluatedKey.pk },
        sk: { S: lastEvaluatedKey.sk }
        };
    }
    return null;
}

function remapLastEvaluatedKeyForNextWeek(lastEvaluatedKey, toHandle, dailyCounter){
  const done = (toHandle <= 0) || (toHandle <= dailyCounter);
  if (done) return null;
  if (lastEvaluatedKey) {
    return { pk: { S: lastEvaluatedKey.pk }, sk: { S: lastEvaluatedKey.sk } };
  }
  return null;
}

async function processItems(paperDeliveryTableName, deliveryWeek, items, step) {
    console.log(`Processing ${items.length} items for step ${step}`);
    const paperDeliveries = items.map(item =>
        utils.mapToPaperDeliveryForGivenStep(item, deliveryWeek, step)
    );
    await dynamo.insertItems(paperDeliveryTableName, paperDeliveries);
}

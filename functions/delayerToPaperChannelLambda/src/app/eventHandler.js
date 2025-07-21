const dynamo = require("./lib/dynamo");
const utils = require("./lib/utils");
const { DayOfWeek, TemporalAdjusters } = require('@js-joda/core');

exports.handleEvent = async (event) => {
    console.log("Event received:", JSON.stringify(event));

    const {
        processType,
        sendToNextWeekCounter,
        lastEvaluatedKeyPhase2,
        lastEvaluatedKeyNextWeek,
        executionDate,
        toNextWeekIncrementCounter,
        toNextStepIncrementCounter
    } = event;

    if (!processType) {
        throw new Error("processType is required in the event");
    }

    const dayOfWeek = parseInt(process.env.PN_DELAYER_DELIVERYDATEDAYOFWEEK, 10) || 1;
    const deliveryWeek = executionDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();

    switch (processType) {
        case "SEND_TO_PHASE_2":
            return await sendToPhase2(deliveryWeek, lastEvaluatedKeyPhase2, toNextStepIncrementCounter);
        case "SEND_TO_NEXT_WEEK":
            return await sendToNextWeek(deliveryWeek, lastEvaluatedKeyNextWeek, sendToNextWeekCounter, toNextWeekIncrementCounter);
        default:
            throw new Error("Invalid processType. Available options are: SEND_TO_PHASE_2, SEND_TO_NEXT_WEEK");
    }
};

async function sendToPhase2(deliveryWeek, lastEvaluatedKeyPhase2, toNextStepIncrementCounter) {
    const processedCount = 0;
    return await retrieveAndProcessItemsToNextStep(
        deliveryWeek,
        lastEvaluatedKeyPhase2,
        toNextStepIncrementCounter,
        parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10),
        processedCount
    );
}

async function sendToNextWeek(deliveryWeek, lastEvaluatedKeyNextWeek, sendToNextWeekCounter, toNextWeekIncrementCounter) {
    const processedCount = 0;
    const queryLimit = Math.min(sendToNextWeekCounter, parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10));
    return await retrieveAndProcessItemsToNextWeek(
        deliveryWeek,
        lastEvaluatedKeyNextWeek,
        sendToNextWeekCounter,
        processedCount,
        toNextWeekIncrementCounter,
        queryLimit
    );
}

async function retrieveAndProcessItemsToNextWeek(deliveryWeek, lastEvaluatedKey, sendToNextWeekCounter, processedCount, toNextWeekIncrementCounter, queryLimit) {
    const response = await dynamo.retrieveItems(deliveryWeek, lastEvaluatedKey, queryLimit, true);

    if (response.Items.length === 0) {
        return {
            toNextWeekIncrementCounter,
            lastEvaluatedKey: null,
            sendToNextWeekCounter
        };
    }

    await processItems(deliveryWeek, response.Items, "EVALUATE_PRINT_CAPACITY");

    processedCount += response.Items.length;
    sendToNextWeekCounter -= response.Items.length;

    if (
        response.LastEvaluatedKey &&
        Object.keys(response.LastEvaluatedKey).length > 0 &&
        processedCount < 4000 &&
        sendToNextWeekCounter > 0
    ) {
        return await retrieveAndProcessItemsToNextWeek(
            deliveryWeek,
            response.LastEvaluatedKey,
            sendToNextWeekCounter,
            processedCount,
            toNextWeekIncrementCounter,
            Math.min(sendToNextWeekCounter, parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10))
        );
    } else {
        return {
            toNextWeekIncrementCounter: toNextWeekIncrementCounter + processedCount,
            lastEvaluatedKey: response.LastEvaluatedKey,
            sendToNextWeekCounter: sendToNextWeekCounter
        };
    }
}

async function retrieveAndProcessItemsToNextStep(deliveryWeek, lastEvaluatedKey, toNextStepIncrementCounter, queryLimit, processedCount) {
    const response = await dynamo.retrieveItems(deliveryWeek, lastEvaluatedKey, queryLimit, false);

    if (response.Items.length === 0) {
        return {
            toNextStepIncrementCounter,
            lastEvaluatedKey: null
        };
    }

    await processItems(deliveryWeek, response.Items, "EVALUATE_SENDER_LIMIT");

    processedCount += response.Items.length;

    if (
        response.LastEvaluatedKey &&
        Object.keys(response.LastEvaluatedKey).length > 0 &&
        processedCount < 4000
    ) {
        return await retrieveAndProcessItemsToNextStep(
            deliveryWeek,
            response.LastEvaluatedKey,
            toNextStepIncrementCounter,
            queryLimit,
            processedCount
        );
    } else {
        return {
            toNextStepIncrementCounter: toNextStepIncrementCounter + processedCount,
            lastEvaluatedKey: response.LastEvaluatedKey
        };
    }
}

async function processItems(deliveryWeek, items, step) {
    const paperDeliveries = items.map(item => utils.mapToPaperDeliveryForGivenStep(item, deliveryWeek, step));
    await dynamo.insertItems(paperDeliveries);
}

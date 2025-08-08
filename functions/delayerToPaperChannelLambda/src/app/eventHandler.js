const dynamo = require("./lib/dynamo");
const utils = require("./lib/utils");
const { DayOfWeek, TemporalAdjusters, Instant, ZoneId } = require('@js-joda/core');

exports.handleEvent = async (event) => {
    console.log("Event received:", JSON.stringify(event));

    if (!event.processType) {
        throw new Error("processType is required in the event");
    }

    const dayOfWeek = parseInt(process.env.PN_DELAYER_DELIVERYDATEDAYOFWEEK, 10) || 1;
    const instant = Instant.parse(event.input.executionDate);
    const zone = ZoneId.systemDefault(); // Or use ZoneId.of("Europe/Rome") if you want to specify
    const deliveryWeek = instant.atZone(zone).toLocalDate()
        .with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)))
        .toString();

    switch (event.processType) {
        case "SEND_TO_PHASE_2": {
            const toSendToNextStep = event.input.dailyPrintCapacity - event.input.sendToNextStepCounter;
            return sendToPhase2(deliveryWeek, event.input, toSendToNextStep);
        }
        case "SEND_TO_NEXT_WEEK": {
            const exceed = event.input.numberOfShipments - event.input.weeklyPrintCapacity;
            const toSendToNextWeek = exceed - event.input.sentToNextWeek - event.input.sendToNextWeekCounter;
            if (toSendToNextWeek > 0) {
                return sendToNextWeek(deliveryWeek, event.input, toSendToNextWeek);
            }
            return {
                input: {
                    dailyPrintCapacity: event.input.dailyPrintCapacity,
                    weeklyPrintCapacity: event.input.weeklyPrintCapacity,
                    numberOfShipments: event.input.numberOfShipments,
                    lastEvaluatedKeyNextWeek: null,
                    sendToNextWeekCounter: event.input.sendToNextWeekCounter,
                    sentToNextWeek: event.input.sentToNextWeek,
                    executionDate: event.input.executionDate
                },
                processType: "SEND_TO_NEXT_WEEK"
            };
        }
        default:
            throw new Error("Invalid processType. Use: SEND_TO_PHASE_2 or SEND_TO_NEXT_WEEK");
    }
};

async function sendToPhase2(deliveryWeek, input, toSendToNextStep) {
    return retrieveAndProcessItems(deliveryWeek,input.lastEvaluatedKeyPhase2,input.sendToNextStepCounter,toSendToNextStep,0,"SENT_TO_PREPARE_PHASE_2", true)
        .then(result => {
            return {
                input: {
                    dailyPrintCapacity: input.dailyPrintCapacity,
                    weeklyPrintCapacity: input.weeklyPrintCapacity,
                    numberOfShipments: input.numberOfShipments,
                    lastEvaluatedKeyPhase2: remapLastEvaluatedKey(result.lastEvaluatedKey),
                    sendToNextStepCounter: result.dailyCounter,
                    executionDate: input.executionDate
                },
                processType: "SEND_TO_PHASE_2"
            };
    });
}

async function sendToNextWeek(deliveryWeek, input, toSendToNextWeek) {
    return retrieveAndProcessItems(deliveryWeek,input.lastEvaluatedKeyNextWeek,input.sendToNextWeekCounter,toSendToNextWeek,0,"EVALUATE_SENDER_LIMIT", false)
        .then(result => {
            return {
                input: {
                    dailyPrintCapacity: input.dailyPrintCapacity,
                    weeklyPrintCapacity: input.weeklyPrintCapacity,
                    numberOfShipments: input.numberOfShipments,
                    lastEvaluatedKeyNextWeek: remapLastEvaluatedKey(result.lastEvaluatedKey),
                    sendToNextWeekCounter: result.dailyCounter,
                    sentToNextWeek: input.sentToNextWeek,
                    executionDate: input.executionDate
                },
                processType: "SEND_TO_NEXT_WEEK"
            };
    });
}

async function retrieveAndProcessItems(deliveryWeek, lastEvaluatedKey, dailyCounter, toHandle, executionCounter, step, scanIndexForward) {
    const limit = Math.min(toHandle, parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10));
    const response = await dynamo.retrieveItems(deliveryWeek, lastEvaluatedKey, limit, scanIndexForward);

    if (!response.Items || response.Items.length === 0) {
        return {
            lastEvaluatedKey: null,
            dailyCounter: dailyCounter
        };
    }

    await processItems(deliveryWeek, response.Items, step);

    const itemsProcessed = response.Items.length;
    dailyCounter += itemsProcessed;
    toHandle -= itemsProcessed;
    executionCounter += itemsProcessed;

    if (
        response.LastEvaluatedKey &&
        Object.keys(response.LastEvaluatedKey).length > 0 &&
        executionCounter < 4000 &&
        toHandle > dailyCounter
    ) {
        return retrieveAndProcessItems(
            deliveryWeek,
            response.LastEvaluatedKey,
            dailyCounter,
            toHandle,
            executionCounter,
            step
        );
    }

    return {
        lastEvaluatedKey: response.LastEvaluatedKey,
        dailyCounter: dailyCounter
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

async function processItems(deliveryWeek, items, step) {
    const paperDeliveries = items.map(item =>
        utils.mapToPaperDeliveryForGivenStep(item, deliveryWeek, step)
    );
    await dynamo.insertItems(paperDeliveries);
}

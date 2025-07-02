const dynamo = require("./lib/dynamo");
const sqsSender = require("./lib/sqsSender");
const { LocalDate } = require("@js-joda/core");
const ssmParameter = require("./lib/ssmParameter");

exports.handleEvent = async () => {

    const priorityMap = await ssmParameter.getPriorityMap();
    const orderedPriorityKeys = Object.keys(priorityMap).sort((a, b) => Number(a) - Number(b));

    const capacityMap = await dynamo.getUsedPrintCapacities();
    let usedDailyCapacity = capacityMap.daily?.usedCapacity;
    let usedWeeklyCapacity = capacityMap.weekly?.usedCapacity;
    let dailyPrintCapacity = capacityMap.daily?.capacity;
    let weeklyPrintCapacity;

    if(!dailyPrintCapacity){
        dailyPrintCapacity = await dynamo.getPrintCapacity();
        usedDailyCapacity = 0;
        usedWeeklyCapacity = 0;
    }

    weeklyPrintCapacity = dailyPrintCapacity * parseInt(process.env.PN_DELAYER_WEEKLY_WORKING_DAYS);

    const executionDate = LocalDate.now().toString();

    let processedCount = 0;
    let remainingDailyCapacity = dailyPrintCapacity - usedDailyCapacity;
    let remainingWeeklyCapacity = weeklyPrintCapacity - usedWeeklyCapacity;

    for (const priorityKey of orderedPriorityKeys) {
        let result = { lastEvaluatedKey: {}, processed: 0 };
        if (remainingDailyCapacity > 0 && remainingWeeklyCapacity > 0) {  
            do {
                result = await retrieveAndProcessItems(priorityKey, executionDate, result.lastEvaluatedKey, Math.min(remainingDailyCapacity, process.env.PAPER_DELIVERY_READYTOSEND_QUERYLIMIT));
                if (!result || !result.processed) break;
                processedCount += result.processed;
                remainingDailyCapacity -= result.processed;
                remainingWeeklyCapacity -= result.processed;
            } while (canProcessMore(remainingDailyCapacity, remainingWeeklyCapacity, processedCount, result));
        }
        if (result.processed === 0) {
            console.log(`No items processed for priority ${priorityKey} and executionDate: ${executionDate}`);
        }else{
            console.log(`Processed ${result.processed} of priority ${priorityKey} items for executionDate: ${executionDate}`);    
        }
        console.log(`Remaining Daily Capacity: ${remainingDailyCapacity} - Remaining Weekly Capacity: ${remainingWeeklyCapacity}`);
    }

    if (remainingDailyCapacity <= 0 && remainingWeeklyCapacity > 0) {
        console.log("Daily print capacity exhausted. Stopping processing for today.");
    } else if (remainingDailyCapacity > 0 && remainingWeeklyCapacity <= 0) {
        console.error("Weekly print capacity exhausted but daily capacity not exhausted. This should not happen.");
        throw new Error("Weekly print capacity exhausted but daily capacity not exhausted. This should not happen.");
    } else {
        console.error("Both daily and weekly print capacities exhausted. Moving all items to incoming.");
        //call job
    }

    if (processedCount > 0) {
        await updatePrintCapacityCounters(executionDate, processedCount);
    }
}

function canProcessMore(remainingDailyCapacity, remainingWeeklyCapacity, processedCount, result) {
    return remainingDailyCapacity > 0 &&
           remainingWeeklyCapacity > 0 &&
           processedCount < 4000 &&
           result.processed > 0 &&
           (result.lastEvaluatedKey && Object.keys(result.lastEvaluatedKey).length !== 0)
}

async function updatePrintCapacityCounters(executionDate, processedCount) {
    await dynamo.updatePrintCapacityCounter(executionDate, "DAY", processedCount);
    await dynamo.updatePrintCapacityCounter(executionDate, "WEEK", processedCount);
    console.log(`used daily capacity incremented of: ${processedCount} - used weekly capacity incremented of: ${processedCount}`);
}

async function retrieveAndProcessItems(priorityKey, executionDate, lastEvaluatedKey, queryLimit) {
    const queryResult = await dynamo.getItems(priorityKey, executionDate, lastEvaluatedKey, queryLimit);
    if (queryResult.Items.length === 0) {
        console.log(`No items found for executionDate: ${executionDate}`);
        return null;
    }

    const sendMessageResponse = await sqsSender.prepareAndSendSqsMessages(queryResult.Items);
    console.log(`Sent ${sendMessageResponse.successes.length} messages`);
    console.log(`Not Sent ${sendMessageResponse.failures.length} messages`);
    const successIds = sendMessageResponse.successes;
    const unprocessedItems = successIds.length
        ? await dynamo.deleteItems(successIds, executionDate)
        : (console.log("No items to delete as no messages were successfully sent."), []);
    console.log(`Deleted ${successIds.length - unprocessedItems.length} items`);

    return {
        processed: successIds.length,
        lastEvaluatedKey: queryResult.LastEvaluatedKey
    };
}
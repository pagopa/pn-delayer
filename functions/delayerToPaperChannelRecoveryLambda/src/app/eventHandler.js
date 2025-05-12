const dynamo = require("./lib/dynamo");
const sqsSender = require("./lib/sqsSender");


exports.handleEvent = async () => {
    const dayToRecovery = process.env.PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE;
    const deliveryDate = dayToRecovery || new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate() - 1)).toISOString().replace(/\.\d{3}Z$/, 'Z');
    await processReadyToSendItems(deliveryDate);
    console.log("Lambda execution completed successfully.");
};

async function processReadyToSendItems(deliveryDate, lastEvaluatedKey) {
    try {
        const { items, lastKey } = await dynamo.getItemsChunk(deliveryDate, lastEvaluatedKey);
        if (items.length === 0) {
            console.log(`No items found for deliveryDate: ${deliveryDate}`);
            return;
        }
        console.log(`Found ${items.length} items for deliveryDate: ${deliveryDate}`);
        const sendMessageResponse = await sqsSender.prepareAndSendSqsMessages(items);
        console.log(`Sent ${sendMessageResponse.successes.length} messages`);
        if(sendMessageResponse.failures.length > 0) {
            console.error(`Not Sent ${sendMessageResponse.failures.length} messages: ${sendMessageResponse.failures}`);
        }
        const successIds = sendMessageResponse.successes;
        const unprocessedItems = successIds.length
            ? await dynamo.deleteItems(successIds, deliveryDate)
            : (console.log("No items to delete as no messages were successfully sent."), []);

        if(unprocessedItems.length > 0) {
          console.error("Failed to delete the following items:", unprocessedItems);
        }
        console.log(`Deleted ${successIds.length - unprocessedItems.length} items`);

        if (lastKey) {
            console.log("Processing next chunk");
            await processReadyToSendItems(lastKey);
        }
    } catch (error) {
        console.error("Error during lambda execution:", error);
        throw error;
    }
}
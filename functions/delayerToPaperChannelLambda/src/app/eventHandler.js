const dynamo = require("./lib/dynamo");
const sqsSender = require("./lib/sqsSender");

exports.handleEvent = async () => {
    try {
        const deliveryDate = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate())).toISOString().replace(/\.\d{3}Z$/, 'Z');
        const items = await dynamo.getItems(deliveryDate);
        if (items.length === 0) {
            console.log(`No items found for deliveryDate: ${deliveryDate}`);
            return;
        }
        const sendMessageResponse = await sqsSender.prepareAndSendSqsMessages(items);
        console.log(`Sent ${sendMessageResponse.successes.length} messages`);
        console.log(`Not Sent ${sendMessageResponse.failures.length} messages`);
        const successIds = sendMessageResponse.successes;
        const unprocessedItems = successIds.length
            ? await dynamo.deleteItems(successIds, deliveryDate)
            : (console.log("No items to delete as no messages were successfully sent."), []);

        console.log(`Deleted ${successIds.length - unprocessedItems.length} items`);

    } catch (error) {
        console.error("Error during lambda execution:", error);
        throw error;
    }
};
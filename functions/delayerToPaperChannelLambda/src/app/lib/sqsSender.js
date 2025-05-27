const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const utils = require("./utils");

const sqsClient = new SQSClient({});

async function prepareAndSendSqsMessages(items) {
    const results = { successes: [], failures: [] };
    const messageBatches = utils.chunkArray(prepareSqsMessages(items),10);

    for (const batch of messageBatches) {
        const batchMap = new Map();
        let idCounter = 0;
        const entries = batch.map(entry => {
            const id = (idCounter++).toString();
            batchMap.set(id, entry);
            return { Id: id, MessageBody: JSON.stringify(entry) };
        });
        try {
            const sqsResponse = await sqsClient.send(new SendMessageBatchCommand({
                QueueUrl: process.env.DELAYERTOPAPERCHANNEL_QUEUEURL,
                Entries: entries,
            }));

            sqsResponse.Failed?.forEach(failure => {
                const item = batchMap.get(failure.Id);
                if (item) results.failures.push(item.requestId);
            });

            sqsResponse.Successful?.forEach(success => {
                const item = batchMap.get(success.Id);
                if (item) results.successes.push(item.requestId);
            });

            console.log(`Successfully sent ${sqsResponse.Successful.length} messages`);
        } catch (error) {
            console.error("Error during SendMessageBatch:", error);
            batch.forEach(item => results.failures.push(item.requestId));
        }
    }
    return results;
}

function prepareSqsMessages(items) {
  return items.map((item) => ({
      requestId: item.requestId,
      iun: item.iun
  }));
}

module.exports = {prepareAndSendSqsMessages};

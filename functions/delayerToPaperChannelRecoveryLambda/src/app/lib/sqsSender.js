const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const utils = require("./utils");

const sqsClient = new SQSClient({});

async function prepareAndSendSqsMessages(items) {
    const results = {
        successes: [],
        failures: [],
    };

    const messageBatches = utils.chunkArray(
        prepareSqsMessages(items).map(item => ({
            Id: item.requestId,
            MessageBody: JSON.stringify(item)
        })),
        10
    );

    for (const batch of messageBatches) {
        try{
            const command = new SendMessageBatchCommand({
                QueueUrl: process.env.DELAYERTOPAPERCHANNEL_QUEUEURL,
                Entries: batch,
              });
            const sqsResponse = await sqsClient.send(command);
            if (sqsResponse.Failed?.length) {
                console.error(`Error sending ${sqsResponse.Failed.length} messages`);
                results.failures.push(...sqsResponse.Failed.map(failure => failure.Id));
            }
            results.successes.push(...sqsResponse.Successful.map(success => success.Id));
            console.log(`Successfully sent ${sqsResponse.Successful.length} messages`);
        } catch (error) {
            console.error("Error during SendMessageBatch:", error);
            results.failures.push(...batch.map(item => item.Id));
        }
    }
    return results;
}

function prepareSqsMessages(items) {
  return items.map((item) => ({
      requestId: item.requestId.S,
      iun: item.iun.S,
      attemptRetry: 0
  }));
}

module.exports = {prepareAndSendSqsMessages};

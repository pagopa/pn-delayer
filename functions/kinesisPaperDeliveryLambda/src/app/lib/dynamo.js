const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  BatchWriteCommand,
  DynamoDBDocumentClient
} = require("@aws-sdk/lib-dynamodb");
const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

async function batchWriteHighPriorityRecords(paperDeliveryHighPriorityRecords) {
  const batch_size = process.env.BATCH_SIZE;
  console.log(`Batch size: ${batch_size}`);
  const tableName = process.env.HIGH_PRIORITY_TABLE_NAME;
  let batchItemFailures = [];

  const params = {
        RequestItems: {
          [tableName]: paperDeliveryHighPriorityRecords.map(record => ({
            PutRequest: {
              Item: record
            }
          }))
        }
  }

  try {
    const command = new BatchWriteCommand(params);
    const response = await docClient.send(command);
    console.log(`Batch write successful for ${paperDeliveryHighPriorityRecords.length} items.`);

    if (response?.UnprocessedItems[tableName]) {
      const writeRequests = response.UnprocessedItems[tableName];
      console.log("error saving highPriorities items totalErrors:" + writeRequests.length);
      batchItemFailures = batchItemFailures.concat(
        writeRequests.map(
          (writeRequest) =>
            writeRequest.PutRequest.Item.requestId.S
        )
      );
      console.warn("batchItemFailures:" + JSON.stringify(batchItemFailures));
    }
  } catch (error) {
    console.error('Error in batch write:', error);
    batchItemFailures = batchItemFailures.concat(paperDeliveryHighPriorityRecords.map(item => item.requestId));
  }
  return batchItemFailures;
}

module.exports = { batchWriteHighPriorityRecords };
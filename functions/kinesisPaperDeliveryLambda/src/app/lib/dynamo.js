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
              Item: record.entity
            }
          }))
        }
  }

  try {
    const command = new BatchWriteCommand(params);
    const response = await docClient.send(command);
    console.log(`Batch write successful for ${paperDeliveryHighPriorityRecords.length} items.`);

    const writeRequests = response.UnprocessedItems[tableName];
    if (writeRequests) {
      const failedIDs = [];
      console.log(`Unprocessed items: ${writeRequests.length}`);
      for (const writeRequest of writeRequests) {
        const unprocessedEntity = writeRequest.PutRequest.Item;
        const failedRecord = paperDeliveryHighPriorityRecords.find(record => record.entity.requestId === unprocessedEntity.requestId.S);
        if (failedRecord) {
          failedIDs.push(failedRecord.kinesisSeqNumber);
        }
      }
      batchItemFailures = batchItemFailures.concat(failedIDs.map((i) => {
        return { itemIdentifier: i };
      }));
      console.warn("batchItemFailures:" + JSON.stringify(batchItemFailures));
    }
  } catch (error) {
    console.error('Error in batch write:', error);
    batchItemFailures = batchItemFailures.concat(paperDeliveryHighPriorityRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; }));
  }
  return batchItemFailures;
}

async function batchWriteKinesisSequenceNumberRecords(eventRecords) {
  const tableName = process.env.KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME;
  const params = {
    RequestItems: {
      [tableName]: eventRecords.map(record => ({
        PutRequest: { Item: record }
      }))
    }
  };
  const command = new BatchWriteCommand(params);
  return await docClient.send(command);
}

async function batchGetKinesisSequenceNumberRecords(keys) {
  const tableName = process.env.KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME;
  const params = {
    RequestItems: {
      [tableName]: {
        Keys: keys.map(key => ({
            {
              sequenceNumber: key
            }
        }))
      }
    }
  };
  const command = new BatchGetItemCommand(params);
  return await client.send(command);
}

module.exports = { batchWriteHighPriorityRecords,
                     batchWriteKinesisSequenceNumberRecords,
                     batchGetKinesisSequenceNumberRecords };
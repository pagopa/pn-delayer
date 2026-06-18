const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const utils = require("./utils");
const {
  DynamoDBDocumentClient,
  QueryCommand,
  BatchWriteCommand,
  UpdateCommand,
} = require("@aws-sdk/lib-dynamodb");
const { LocalDate } = require('@js-joda/core');

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

async function updateSenderPriorityCounter(paperDeliveryCounterTableName, groupedSenderPaIdRecords, deliveryWeek, batchItemFailures) {
  let deliveryWeekLocalDate = LocalDate.parse(deliveryWeek);
  const deliveryDate = deliveryWeekLocalDate.plusDays(7).toString();

  for (const [senderPaId, records] of Object.entries(groupedSenderPaIdRecords)) {
    const sk = `SENDER_PRIORITY~${senderPaId}`;
    try {
      const priorities = new Set(
        records
          .map(r => r.senderPriority)
          .filter(p => p !== 0)
      );
      console.log(`Updating sender priority counter for senderPaId: ${senderPaId} with priorities: ${JSON.stringify(Array.from(priorities))}`);

      if (!priorities || priorities.size === 0) {
        console.log(`Skipping updating sender priority for senderPaId: ${senderPaId}`);
        continue;
      }

      const input = {
        TableName: paperDeliveryCounterTableName,
        Key: {
          pk: deliveryDate,
          sk: sk
        },
        UpdateExpression: 'ADD #priorities :priorities SET #paId = :paId',
        ExpressionAttributeNames: {
          '#priorities': 'priorities',
          '#paId': 'paId'
        },
        ExpressionAttributeValues: {
          ':priorities': priorities,
          ':paId': senderPaId
        }
      };
      const command = new UpdateCommand(input);
      await docClient.send(command);
      console.log(`updateSuccessfully for ${sk}`);
    } catch (error) {
      console.error(`Failed to update sender priority counter for sk: ${sk}`, error);
      const failedSeqNumbers = records.map(i => ({ itemIdentifier: i.kinesisSeqNumber }));
      batchItemFailures.push(...failedSeqNumbers);
    }
  }
  return batchItemFailures;
}

async function retrieveItems(paperDeliveryTableName, deliveryWeek, LastEvaluatedKey, limit, scanIndexForward) {

  const partitionKey = `${deliveryWeek}~EVALUATE_PRINT_CAPACITY`

  const params = {
    TableName: paperDeliveryTableName,
    KeyConditionExpression: "pk = :partitionKey",
    ExpressionAttributeValues: {
      ":partitionKey": partitionKey
    },
    ScanIndexForward: scanIndexForward,
    Limit: parseInt(limit, 10)
  };

  if (LastEvaluatedKey && Object.keys(LastEvaluatedKey).length > 0) {
    params.ExclusiveStartKey = removeDynamoTypes(LastEvaluatedKey);
  }

  const result = await docClient.send(new QueryCommand(params));
  return result || {Items: [], LastEvaluatedKey: {} };
}

function removeDynamoTypes(lastEvaluatedKey){
    const result = {};
    for (const key in lastEvaluatedKey) {
      result[key] = Object.values(lastEvaluatedKey[key])[0];
    }
    return result;
};

async function insertItems(paperDeliveryTableName, items) {
    const putRequests = items.map(item => ({
        PutRequest: {
            Item: item
        }
    }));
    return await insertItemsBatch(paperDeliveryTableName, putRequests, 1);
}

async function insertItemsBatch(paperDeliveryTableName, putRequests, retryCount) {
    let unprocessedRequests = [];
    const chunks = utils.chunkArray(putRequests, 25);
    for (const chunk of chunks) {
        try {
            console.log(`Inserting ${chunk.length} items`);
            let input = {
                RequestItems: {
                    [paperDeliveryTableName] : chunk }
            };
            const result = await docClient.send(new BatchWriteCommand(input));
            unprocessedRequests.push(
                ...(result.UnprocessedItems?.[paperDeliveryTableName] || [])
            );
        } catch (error) {
            console.error("Error during batch insert:", error);
            unprocessedRequests.push(...(chunk))
        }
    }
    if (unprocessedRequests.length > 0 && retryCount < 3) {
        console.log(`Retrying ${unprocessedRequests.length} unprocessed items`);
        return insertItemsBatch(paperDeliveryTableName, unprocessedRequests, retryCount + 1);
    }

    if (retryCount >= 3 && unprocessedRequests.length > 0) {
        console.error(`Failed to insert ${unprocessedRequests.length} items after 3 attempts`);
        return unprocessedRequests;
    } else {
        console.log("All items inserted successfully.");
        return unprocessedRequests;
    }
}


module.exports = { retrieveItems, insertItems, updateSenderPriorityCounter };
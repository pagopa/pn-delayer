const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const utils = require("./utils");
const {
  DynamoDBDocumentClient,
  QueryCommand,
  BatchWriteCommand,
} = require("@aws-sdk/lib-dynamodb");

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const paperDeliveryTableName = process.env.PAPER_DELIVERY_TABLENAME;

async function retrieveItems(deliveryWeek, LastEvaluatedKey, limit, scanIndexForward) {

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

async function insertItems(items) {
    const putRequests = items.map(item => ({
        PutRequest: {
            Item: item
        }
    }));
    return await insertItemsBatch(putRequests, 1);
}

async function insertItemsBatch(putRequests, retryCount) {
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
        return insertItemsBatch(unprocessedRequests, retryCount + 1);
    }

    if (retryCount >= 3 && unprocessedRequests.length > 0) {
        console.error(`Failed to insert ${unprocessedRequests.length} items after 3 attempts`);
        return unprocessedRequests;
    } else {
        console.log("All items inserted successfully.");
        return unprocessedRequests;
    }
}


module.exports = { retrieveItems, insertItems };
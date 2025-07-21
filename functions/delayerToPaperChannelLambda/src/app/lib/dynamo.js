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

  const partitionKey = `${deliveryWeek}~EVALUATE_SENDER_LIMIT`

  const params = {
    TableName: paperDeliveryTableName,
    KeyConditionExpression: "pk = :partitionKey AND deliveryDate <= :executionDate",
    ExpressionAttributeValues: {
      ":partitionKey": partitionKey
    },
    ScanIndexForward: scanIndexForward,
    ExclusiveStartKey: LastEvaluatedKey || undefined,
    Limit: parseInt(limit, 10)
  };

  const result = await docClient.send(new QueryCommand(params));
  return result || {Items: [], LastEvaluatedKey: {} };
}


async function insertItems(items) {
    const putRequests = items.map(item => ({
            PutRequests: item
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
            console.error("Error during batch delete:", error);
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
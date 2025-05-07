const { DynamoDBClient, BatchWriteItemCommand } = require("@aws-sdk/client-dynamodb");
const utils = require("./utils");
const {
  DynamoDBDocumentClient,
  QueryCommand
} = require("@aws-sdk/lib-dynamodb");

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const readyToSendTableName = process.env.PAPERDELIVERYREADYTOSEND_TABLENAME;

async function getItems(deliveryDate) {
  const params = {
    TableName: readyToSendTableName,
    KeyConditionExpression: "deliveryDate = :midnightTimestamp",
    ExpressionAttributeValues: {
      ":midnightTimestamp": deliveryDate,
    },
    Limit: parseInt(process.env.PAPERDELIVERYREADYTOSEND_QUERYLIMIT, 10)
  };

  const result = await docClient.send(new QueryCommand(params)); return result?.Items || []; }


async function deleteItems(requestIds, deliveryDate) {
    const deleteRequests = requestIds.map(requestId => ({
            DeleteRequest: { Key: { deliveryDate : {S:deliveryDate}, requestId: {S:requestId} } }
        }));
    return await deleteItemsBatch(deleteRequests, 1);
}

async function deleteItemsBatch(deleteRequests, retryCount) {
    let unprocessedRequests = [];
    const chunks = utils.chunkArray(deleteRequests, 25);
    for (const chunk of chunks) {
        try {
            console.log(`Deleting ${chunk.length} items`);
            let input = {
                RequestItems: {
                    [readyToSendTableName] : chunk }
            };
            const result = await client.send(new BatchWriteItemCommand(input));
            unprocessedRequests.push(
                ...(result.UnprocessedItems?.[readyToSendTableName] || [])
            );
        } catch (error) {
            console.error("Error during batch delete:", error);
            unprocessedRequests.push(...(chunk))
        }
    }
    if (unprocessedRequests.length > 0 && retryCount < 3) {
        console.log(`Retrying ${unprocessedRequests.length} unprocessed items`);
        return deleteItemsBatch(unprocessedRequests, retryCount + 1);
    }

    if (retryCount >= 3 && unprocessedRequests.length > 0) {
        console.error(`Failed to delete ${unprocessedRequests.length} items after 3 attempts`);
        return unprocessedRequests;
    } else {
        console.log("All items deleted successfully.");
        return unprocessedRequests;
    }
}

module.exports = { getItems , deleteItems};
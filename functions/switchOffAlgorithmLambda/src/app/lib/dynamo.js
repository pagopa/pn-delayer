const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { chunkArray } = require('./utils');

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const TABLE_NAME = process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;
const QUERY_LIMIT = Number.parseInt(process.env.QUERY_LIMIT, 10) || 1000;

async function queryByPartitionKey(partitionKey, executionLimit, lastEvaluatedKey) {
    const limit = Math.min(QUERY_LIMIT, executionLimit);
    const params = {
      TableName: TABLE_NAME,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': partitionKey },
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    return {
      items: result.Items,
      lastEvaluatedKey: result.LastEvaluatedKey,
    };
}

async function insertItemsBatch(putRequests, retryCount = 0) {
  let unprocessed = [];
  const chunks = chunkArray(putRequests, 25);

  for (const chunk of chunks) {
    try {
      const input = { RequestItems: { [TABLE_NAME]: chunk } };
      const result = await docClient.send(new BatchWriteCommand(input));

      unprocessed.push(...(result.UnprocessedItems?.[TABLE_NAME] ?? []));
    } catch (err) {
      console.error("Batch write error:", err);
      unprocessed.push(...chunk);
    }
  }

  if (unprocessed.length > 0 && retryCount < 3) {
    return insertItemsBatch(unprocessed, retryCount + 1);
  }

  return unprocessed;
}

module.exports = {
  queryByPartitionKey,
  insertItemsBatch
};

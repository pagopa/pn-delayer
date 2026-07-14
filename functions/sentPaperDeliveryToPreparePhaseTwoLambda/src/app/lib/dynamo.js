const {
  DynamoDBClient,
  QueryCommand,
  BatchWriteItemCommand,
  TransactWriteItemsCommand
} = require('@aws-sdk/client-dynamodb');

const { marshall, unmarshall } = require('@aws-sdk/util-dynamodb');
const { chunkArray } = require('./utils');

const dynamoClient = new DynamoDBClient({});

const REQUEST_ID_INDEX_NAME = 'requestId-CreatedAt-index';
const MAX_BATCH_WRITE_ITEMS = 25;
const MAX_TRANSACTION_ITEMS = 100;
const MAX_ITEMS_TO_MOVE_PER_TRANSACTION = MAX_TRANSACTION_ITEMS / 2;
const MAX_BATCH_RETRIES = 3;

function getTableName() {
  const tableName = process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;

  if (!tableName) {
    throw new Error('DELAYER_PAPER_DELIVERY_TABLE_NAME not found');
  }

  return tableName;
}

async function queryLatestByRequestId(requestId) {
  if (!requestId) {
    throw new Error('requestId is required');
  }

  const response = await dynamoClient.send(
    new QueryCommand({
      TableName: getTableName(),
      IndexName: REQUEST_ID_INDEX_NAME,
      KeyConditionExpression: 'requestId = :requestId',
      ExpressionAttributeValues: marshall({
        ':requestId': requestId
      }),
      ScanIndexForward: false,
      Limit: 1
    })
  );

  return response.Items?.[0] ? unmarshall(response.Items[0]) : null;
}

async function insertItemsBatch(putRequests, retryCount = 0) {
  if (!Array.isArray(putRequests) || putRequests.length === 0) {
    return [];
  }

  const tableName = getTableName();
  const unprocessed = [];

  for (const chunk of chunkArray(putRequests, MAX_BATCH_WRITE_ITEMS)) {
    const response = await dynamoClient.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [tableName]: chunk.map(({ PutRequest }) => ({
            PutRequest: {
              Item: marshall(PutRequest.Item, {
                removeUndefinedValues: true
              })
            }
          }))
        }
      })
    );

    const currentUnprocessed = response.UnprocessedItems?.[tableName] ?? [];

    unprocessed.push(
      ...currentUnprocessed.map(({ PutRequest }) => ({
        PutRequest: {
          Item: unmarshall(PutRequest.Item)
        }
      }))
    );
  }

  if (unprocessed.length > 0 && retryCount < MAX_BATCH_RETRIES) {
    await sleep(2 ** retryCount * 1000);
    return insertItemsBatch(unprocessed, retryCount + 1);
  }

  return unprocessed;
}

async function moveItemsToDeleted(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return;
  }

  const tableName = getTableName();

  for (const chunk of chunkArray(items, MAX_ITEMS_TO_MOVE_PER_TRANSACTION)) {
    await dynamoClient.send(
      new TransactWriteItemsCommand({
        TransactItems: chunk.flatMap(item => buildMoveToDeletedTransaction(item, tableName))
      })
    );
  }
}

function buildMoveToDeletedTransaction(item, tableName) {
  if (!item.pk || !item.sk) {
    throw new Error(`Item senza pk/sk: requestId=${item.requestId}`);
  }

  return [
    {
      Delete: {
        TableName: tableName,
        Key: marshall({
          pk: item.pk,
          sk: item.sk
        })
      }
    },
    {
      Put: {
        TableName: tableName,
        Item: marshall(
          {
            ...item,
            pk: `DELETED#${item.pk}`
          },
          {
            removeUndefinedValues: true
          }
        )
      }
    }
  ];
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = {
  queryLatestByRequestId,
  insertItemsBatch,
  moveItemsToDeleted
};
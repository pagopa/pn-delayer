const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  TransactWriteCommand,
  QueryCommand,
  DynamoDBDocumentClient
} = require("@aws-sdk/lib-dynamodb");
const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: {
    removeUndefinedValues: true,
  }});
const paperDeliveryTable = process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;

async function retrievePaperDelivery(requestId) {
  const queryParams = {
    TableName: paperDeliveryTable,
    IndexName: "requestId-CreatedAt-index",
    KeyConditionExpression: "requestId = :requestId",
    ExpressionAttributeValues: {
      ":requestId": requestId
    },
    ScanIndexForward: false,
    Limit: 1
  };

  const result = await docClient.send(new QueryCommand(queryParams));

  if (!result.Items || result.Items.length === 0) {
    return null;
  }

  return result.Items[0];
}

async function executeTransactions(paperDeliveryItems, kinesisSequenceNumber) {
  if (!paperDeliveryItems || paperDeliveryItems.length === 0) {
    return {
      success: true,
      kinesisSequenceNumber
    };
  }

  const transactItems = [];

  for (const item of paperDeliveryItems) {
    const { pk, sk } = item;

    transactItems.push({
      Delete: {
        TableName: paperDeliveryTable,
        Key: { pk, sk }
      }
    });

    transactItems.push({
      Put: {
        TableName: paperDeliveryTable,
        Item: {
          ...item,
          pk: `DELETED~${pk}`
        }
      }
    });
  }

  try {
    await docClient.send(
      new TransactWriteCommand({
        TransactItems: transactItems
      })
    );

    return {
      success: true,
      kinesisSequenceNumber
    };
  } catch (error) {
    console.error("TransactWrite failed", { error,kinesisSequenceNumber });
    return {
      success: false,
      kinesisSequenceNumber
    };
  }
}

module.exports = { executeTransactions,  retrievePaperDelivery};
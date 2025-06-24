const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

async function retrieveProvinceWithPaperDeliveries(deliveryWeek) {
  const params = {
    TableName: process.env.PAPER_DELIVERY_COUNTER_TABLENAME,
    KeyConditionExpression: "#week = :week AND begins_with(#sk, :prefix)",
    ExpressionAttributeNames: {
      "#week": "deliveryDate",
      "#sk": "sk"
    },
    ExpressionAttributeValues: {
      ":week": deliveryWeek,
      ":prefix": "EVAL"
    }
  };

  const command = new QueryCommand(params);
  const response = await docClient.send(command);
  return response.Items;
}

module.exports = { retrieveProvinceWithPaperDeliveries };
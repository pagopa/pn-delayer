"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_PAPER_DELIVERY operation
 * @param {Array<string>} params [deliveryDate, workflowStep, lastEvaluatedKey]
 */
async function getPaperDelivery(params = []) {
    const [paperDeliveryTableName, deliveryDate, workflowStep, lastEvaluatedKey] = params;
    if (!paperDeliveryTableName || !deliveryDate || !workflowStep) {
        throw new Error("Required parameters are [paperDeliveryTableName, deliveryDate, workflowStep]");
    }

    const pk = `${deliveryDate}~${workflowStep}`;
    const limit = parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10);
    const queryParams = {
      TableName: paperDeliveryTableName,
      KeyConditionExpression: "pk = :pk",
      ExpressionAttributeValues: {
          ":pk": pk,
      },
      Limit: limit,
    };

    if (lastEvaluatedKey) {
      queryParams.ExclusiveStartKey = lastEvaluatedKey;
    }

    const command = new QueryCommand(queryParams);

    const { Items, LastEvaluatedKey } = await docClient.send(command);
    if (!Items || Items.length === 0) {
        return { items: [] };
    }
    return { items: Items, lastEvaluatedKey: LastEvaluatedKey };
}

module.exports = { getPaperDelivery };
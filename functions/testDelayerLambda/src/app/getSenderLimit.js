"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-PaperDeliverySenderLimit";
const GSI_NAME = "deliveryDateProvince-index";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_SENDER_LIMIT operation
 * @param {Array<string>} params [deliveryDate, province, lastEvaluatedKey]
 */
async function getSenderLimit(params = []) {
    const [deliveryDate, province, lastEvaluatedKey] = params;
    if (!deliveryDate || !province) {
        throw new Error("Parameters must be [deliveryDate, province]");
    }

    const limit = parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10);
    const queryParams = {
      TableName: TABLE_NAME,
      IndexName: GSI_NAME,
      KeyConditionExpression: "deliveryDate = :deliveryDate AND province = :province",
      ExpressionAttributeValues: {
        ":deliveryDate": deliveryDate,
        ":province": province,
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

module.exports = { getSenderLimit };
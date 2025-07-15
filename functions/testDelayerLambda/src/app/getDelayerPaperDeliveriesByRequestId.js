"use strict";

const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-DelayerPaperDelivery";
const INDEX_NAME = "requestId-CreatedAt-index";
const docClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));

/**

 Ritorna tutte le righe che condividono lo stesso requestId.

 @param {Array} params [requestId]
 */
async function getDelayerPaperDeliveriesByRequestId([requestId] = []) {
    if (!requestId) {
        throw new Error("Parameter must be [requestId]");
    }

    const cmd = new QueryCommand({
        TableName: TABLE_NAME,
        IndexName: INDEX_NAME,
        KeyConditionExpression: "#r = :rid",
        ExpressionAttributeNames: { "#r": "requestId" },
        ExpressionAttributeValues: { ":rid": requestId },
    });

    const { Items } = await docClient.send(cmd);
    return Items || [];
}

module.exports = { getDelayerPaperDeliveriesByRequestId };
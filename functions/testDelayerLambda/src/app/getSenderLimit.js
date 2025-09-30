"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-PaperDeliverySenderLimit";
const GSI_NAME = "deliveryDateProvince-index";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_SENDER_LIMIT operation
 * @param {Array<string>} params [deliveryDate, province]
 */
async function getSenderLimit(params = []) {
    const [deliveryDate, province] = params;
    if (!deliveryDate || !province) {
        throw new Error("Parameters must be [deliveryDate, province]");
    }
    const command = new QueryCommand({
        TableName: TABLE_NAME,
        IndexName: GSI_NAME,
        KeyConditionExpression: "deliveryDate = :deliveryDate AND province = :province",
        ExpressionAttributeValues: {
            ":deliveryDate": deliveryDate,
            ":province": province,
        },
    });
    const { Items } = await docClient.send(command);
    if (!Items || Items.length === 0) {
        return { message: "No items found" };
    }
    return Items;
}

module.exports = { getSenderLimit };
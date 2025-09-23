"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-PaperDeliverySenderLimit";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_SENDER_LIMIT operation
 * @param {Array<string>} params [paId, productType, province, deliveryDate]
 */
async function getSenderLimit(params = []) {
    const [paId, productType, province, deliveryDate] = params;
    if (!paId || !productType || !province || !deliveryDate) {
        throw new Error("Parameters must be [paId, productType, province, deliveryDate]");
    }
    const partitionKey = `${paId}~${productType}~${province}`;
    const command = new GetCommand({
        TableName: TABLE_NAME,
        Key: {
            pk: partitionKey,
            deliveryDate: deliveryDate,
        },
    });
    const { Item } = await docClient.send(command);
    if (!Item) {
        return { message: "Item not found" };
    }
    return Item;
}

module.exports = { getSenderLimit };
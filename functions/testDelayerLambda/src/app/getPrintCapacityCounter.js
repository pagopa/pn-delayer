"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    GetCommand
} = require("@aws-sdk/lib-dynamodb");


const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_PRINT_CAPACITY_COUNTER operation
 * @param {Array<string>} params [countersTableName, deliveryDate]
 */
async function getPrintCounter(params = []) {
    const [countersTableName, deliveryDate] = params;
    if (!countersTableName || !deliveryDate) {
        throw new Error("Required parameters are [paperDeliveryCounters, deliveryDate]");
    }
    const command = new GetCommand({
        TableName: countersTableName,
        Key: {
            pk: "PRINT",
            sk: deliveryDate,
        },
    });
    const { Item } = await docClient.send(command);
    if (!Item) {
        return { message: "Item not found" };
    }
    return Item;
}

module.exports = { getPrintCounter };
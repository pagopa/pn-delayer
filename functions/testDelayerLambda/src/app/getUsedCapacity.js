"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-PaperDeliveryDriverUsedCapacities";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_USED_CAPACITY operation
 * @param {Array<string>} params [unifiedDeliveryDriver, geoKey, deliveryDate]
 */
async function getUsedCapacity(params = []) {
    const [unifiedDeliveryDriver, geoKey, deliveryDate] = params;
    if (!unifiedDeliveryDriver || !geoKey || !deliveryDate) {
        throw new Error("Parameters must be [unifiedDeliveryDriver, geoKey, deliveryDate]");
    }
    const partitionKey = `${unifiedDeliveryDriver}~${geoKey}`;
    const command = new GetCommand({
        TableName: TABLE_NAME,
        Key: {
            unifiedDeliveryDriverGeokey: partitionKey,
            deliveryDate: deliveryDate,
        },
    });
    const { Item } = await docClient.send(command);
    if (!Item) {
        return { message: "Item not found" };
    }
    return Item;
}

module.exports = { getUsedCapacity };
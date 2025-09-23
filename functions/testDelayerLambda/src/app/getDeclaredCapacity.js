"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-PaperDeliveryDriverCapacities";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_DECLARED_CAPACITY operation
 * @param {Array<string>} params [tenderId, unifiedDeliveryDriver, geoKey, deliveryDate]
 */
async function getDeclaredCapacity(params = []) {
    const [tenderId, unifiedDeliveryDriver, geoKey, deliveryDate] = params;
    if (!tenderId || !unifiedDeliveryDriver || !geoKey || !deliveryDate) {
        throw new Error("Parameters must be [tenderId, unifiedDeliveryDriver, geoKey, deliveryDate]");
    }

    const partitionKey = `${tenderId}~${unifiedDeliveryDriver}~${geoKey}`;

    const command = new QueryCommand({
        TableName: TABLE_NAME,
        KeyConditionExpression: "pk = :pk AND activationDateFrom <= :deliveryDate",
        FilterExpression: "attribute_not_exists(activationDateTo) OR activationDateTo >= :now",
        ExpressionAttributeValues: {
            ":pk": partitionKey,
            ":deliveryDate": deliveryDate,
            ":now": deliveryDate
        },
        Limit: 1,
        ScanIndexForward: false
    });

    return docClient.send(command);
}

module.exports = { getDeclaredCapacity };
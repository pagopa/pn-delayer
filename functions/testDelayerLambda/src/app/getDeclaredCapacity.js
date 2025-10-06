"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");

const GSI_NAME = "tenderIdGeoKey-index";
const TENDER_API_LAMBDA_ARN = process.env.PAPERCHANNELTENDERAPI_LAMBDA_ARN;
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);
const lambdaClient = new LambdaClient({});

/**
 * GET_DECLARED_CAPACITY operation
 * @param {Array<string>} params [paperDeliveryDriverCapacitiesTabelName, province, deliveryDate]
 */
async function getDeclaredCapacity(params = []) {
    const [paperDeliveryDriverCapacitiesTabelName, province, deliveryDate] = params;
    if (!paperDeliveryDriverCapacitiesTabelName || !province || !deliveryDate) {
        throw new Error("Parameters must be [paperDeliveryDriverCapacitiesTabelName, province, deliveryDate]");
    }

    const tenderId = await getActiveTender();
    if (!tenderId) {
        throw new Error("No active tender found");
    }
    const partitionKey = `${tenderId}~${province}`;

    const command = new QueryCommand({
        TableName: paperDeliveryDriverCapacitiesTabelName,
        IndexName: GSI_NAME,
        KeyConditionExpression: "tenderIdGeoKey = :pk AND activationDateFrom < :deliveryDate",
        FilterExpression: "attribute_not_exists(activationDateTo) OR activationDateTo > :deliveryDate",
        ExpressionAttributeValues: {
            ":pk": partitionKey,
            ":deliveryDate": deliveryDate
        },
        Limit: 1000
    });

    const result = await docClient.send(command);

    const groupedByDriver = {};

    for (const item of result.Items || []) {
        const driver = item.unifiedDeliveryDriver;

        if (!groupedByDriver[driver] ||
            item.activationDateFrom > groupedByDriver[driver].activationDateFrom) {
            groupedByDriver[driver] = item;
        }
    }

    return Object.values(groupedByDriver);
}

/**
 * Invoke TenderApiLambda to get tender information
 * @param {string} operation - The operation to perform (e.g., 'GET_TENDER_ACTIVE')
 */
async function getActiveTender() {
    const command = new InvokeCommand({
            FunctionName: TENDER_API_LAMBDA_ARN,
            InvocationType: "RequestResponse",
            Payload: JSON.stringify({
                operation: "GET_TENDER_ACTIVE"
            })
        });

    const response = await lambdaClient.send(command);
    const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

    return responsePayload.body.tenderId;
}

module.exports = { getDeclaredCapacity };
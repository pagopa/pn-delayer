"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    GetCommand,
    QueryCommand
} = require("@aws-sdk/lib-dynamodb");

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);
const limit = parseInt(process.env.PAPER_DELIVERY_COUNTERS_QUERYLIMIT || '1000', 10);

async function getCounters(params = {}) {
    const { table, counterType, deliveryDate, province, productType, lastEvaluatedKey} = params;

    if (!table || !deliveryDate || !counterType) {
        throw new Error("Required parameters are [table, deliveryDate, counterType]");
    }

    switch (counterType) {
        case "PRINT":
            return handlePrint(params);

        case "SUM_ESTIMATE":
            return handleSumEstimate(params);

        case "EXCLUDE":
            return handleExclude(params);

        default:
            throw new Error(`Unsupported counterType: ${counterType}`);
    }
}


//PRINT
async function handlePrint({ table, deliveryDate }) {
    return executeGet({
        table,
        pk: "PRINT",
        sk: deliveryDate
    });
}

//SUM_ESTIMATE
async function handleSumEstimate({ table, deliveryDate, province, productType, lastEvaluatedKey }) {
    let skPrefix;

    if (province == null && productType == null) {
        skPrefix = "SUM_ESTIMATE";
    } else if (province == null && productType != null) {
        skPrefix = `SUM_ESTIMATE~${productType}`;
    } else if (province != null && productType != null) {
        skPrefix = `SUM_ESTIMATE~${productType}~${province}`;
    } else {
        throw new Error("productType is required when province is provided for SUM_ESTIMATE counter");
    }

    return executeQuery({
        table,
        pk: deliveryDate,
        skPrefix,
        lastEvaluatedKey
    });
}

//EXCLUDE
async function handleExclude({ table, deliveryDate, province, productType, lastEvaluatedKey }) {

    if (province != null && productType != null) {
        return executeGet({
            table,
            pk: deliveryDate,
            sk: `EXCLUDE~${province}~${productType}`
        });
    }

    let skPrefix;

    if (province == null && productType == null) {
        skPrefix = "EXCLUDE";
    } else if (province != null && productType == null) {
        skPrefix = `EXCLUDE~${province}`;
    } else {
        throw new Error("province is required when productType is provided for EXCLUDE counter");
    }

    return executeQuery({
        table,
        pk: deliveryDate,
        skPrefix,
        lastEvaluatedKey
    });
}

//QUERY
async function executeQuery({ table, pk, skPrefix, lastEvaluatedKey }) {
    const queryParams = {
        TableName: table,
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :sk)",
        ExpressionAttributeValues: {
            ":pk": pk,
            ":sk": skPrefix,
        },
        Limit: limit,
    };

    if (lastEvaluatedKey) {
        queryParams.ExclusiveStartKey = lastEvaluatedKey;
    }

    const command = new QueryCommand(queryParams);
    const { Items, LastEvaluatedKey } = await docClient.send(command);

    return {
        items: Items || [],
        lastEvaluatedKey: LastEvaluatedKey,
    };
}

//GET
async function executeGet({ table, pk, sk }) {
    const command = new GetCommand({
        TableName: table,
        Key: { pk, sk },
    });

    const { Item } = await docClient.send(command);

    if (!Item) {
        return { message: "Item not found" };
    }

    return Item;
}

module.exports = { getCounters };
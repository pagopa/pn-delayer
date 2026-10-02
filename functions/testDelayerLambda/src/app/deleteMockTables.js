"use strict";

const {
    DynamoDBClient,
    CreateTableCommand,
    DeleteTableCommand,
    DescribeTableCommand,
    ResourceNotFoundException
} = require("@aws-sdk/client-dynamodb");

const ddbClient = new DynamoDBClient({});
const ACTIVE_STATUS = "ACTIVE";
const DELETING_STATUS = "DELETING";
const MOCK_TABLE_PREFIX = "pn";
const WAIT_DELAY_MS = Number(process.env.DELETE_MOCK_TABLES_WAIT_DELAY_MS || 1000);
const MAX_WAIT_ATTEMPTS = Number(process.env.DELETE_MOCK_TABLES_MAX_WAIT_ATTEMPTS || 120);

const buildTableDefinitions = () => [
    {
        TableName: `${MOCK_TABLE_PREFIX}-DelayerPaperDeliveryMock`,
        AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
            { AttributeName: "requestId", AttributeType: "S" },
            { AttributeName: "createdAt", AttributeType: "S" }
        ],
        KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" }
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: "requestId-CreatedAt-index",
                KeySchema: [
                    { AttributeName: "requestId", KeyType: "HASH" },
                    { AttributeName: "createdAt", KeyType: "RANGE" }
                ],
                Projection: { ProjectionType: "ALL" }
            }
        ],
        BillingMode: "PAY_PER_REQUEST",
        StreamSpecification: {
            StreamEnabled: true,
            StreamViewType: "NEW_IMAGE"
        }
    },
    {
        TableName: `${MOCK_TABLE_PREFIX}-PaperDeliveryCountersMock`,
        AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" }
        ],
        KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" }
        ],
        BillingMode: "PAY_PER_REQUEST"
    },
    {
        TableName: `${MOCK_TABLE_PREFIX}-PaperDeliveryDriverCapacitiesMock`,
        AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "activationDateFrom", AttributeType: "S" },
            { AttributeName: "tenderIdGeoKey", AttributeType: "S" }
        ],
        KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "activationDateFrom", KeyType: "RANGE" }
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: "tenderIdGeoKey-index",
                KeySchema: [
                    { AttributeName: "tenderIdGeoKey", KeyType: "HASH" },
                    { AttributeName: "activationDateFrom", KeyType: "RANGE" }
                ],
                Projection: { ProjectionType: "ALL" }
            }
        ],
        BillingMode: "PAY_PER_REQUEST"
    },
    {
        TableName: `${MOCK_TABLE_PREFIX}-PaperDeliveryDriverUsedCapacitiesMock`,
        AttributeDefinitions: [
            { AttributeName: "unifiedDeliveryDriverGeokey", AttributeType: "S" },
            { AttributeName: "deliveryDate", AttributeType: "S" }
        ],
        KeySchema: [
            { AttributeName: "unifiedDeliveryDriverGeokey", KeyType: "HASH" },
            { AttributeName: "deliveryDate", KeyType: "RANGE" }
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: "deliveryDate-index",
                KeySchema: [
                    { AttributeName: "deliveryDate", KeyType: "HASH" }
                ],
                Projection: { ProjectionType: "ALL" }
            }
        ],
        BillingMode: "PAY_PER_REQUEST"
    },
    {
        TableName: `${MOCK_TABLE_PREFIX}-PaperDeliverySenderLimitMock`,
        AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "deliveryDate", AttributeType: "S" },
            { AttributeName: "province", AttributeType: "S" },
            { AttributeName: "fileKey", AttributeType: "S" }
        ],
        KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "deliveryDate", KeyType: "RANGE" }
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: "deliveryDateProvince-index",
                KeySchema: [
                    { AttributeName: "deliveryDate", KeyType: "HASH" },
                    { AttributeName: "province", KeyType: "RANGE" }
                ],
                Projection: { ProjectionType: "ALL" }
            },
            {
                IndexName: "fileKey-index",
                KeySchema: [
                    { AttributeName: "fileKey", KeyType: "HASH" }
                ],
                Projection: { ProjectionType: "ALL" }
            }
        ],
        BillingMode: "PAY_PER_REQUEST"
    },
    {
        TableName: `${MOCK_TABLE_PREFIX}-PaperDeliveryUsedSenderLimitMock`,
        AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "deliveryDate", AttributeType: "S" },
            { AttributeName: "province", AttributeType: "S" }
        ],
        KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "deliveryDate", KeyType: "RANGE" }
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: "deliveryDate-province-index",
                KeySchema: [
                    { AttributeName: "deliveryDate", KeyType: "HASH" },
                    { AttributeName: "province", KeyType: "RANGE" }
                ],
                Projection: { ProjectionType: "ALL" }
            }
        ],
        BillingMode: "PAY_PER_REQUEST"
    }
];

exports.deleteMockTables = async () => {
    const tableDefinitions = buildTableDefinitions();

    for (const tableDefinition of tableDefinitions) {
        await recreateTable(tableDefinition);
    }

    return {
        message: "Mock tables recreated",
        tables: tableDefinitions.map(({ TableName }) => TableName)
    };
};

async function recreateTable(tableDefinition) {
    const tableName = tableDefinition.TableName;
    const status = await getTableStatus(tableName);

    if (status && status !== DELETING_STATUS) {
        if (status !== ACTIVE_STATUS) {
            await waitForTableStatus(tableName, ACTIVE_STATUS);
        }
        await ddbClient.send(new DeleteTableCommand({ TableName: tableName }));
    }

    await waitForTableDeletion(tableName);
    await ddbClient.send(new CreateTableCommand(tableDefinition));
    await waitForTableStatus(tableName, ACTIVE_STATUS);
}

async function getTableStatus(tableName) {
    try {
        const result = await ddbClient.send(new DescribeTableCommand({ TableName: tableName }));
        return result.Table?.TableStatus;
    } catch (err) {
        if (isResourceNotFound(err)) {
            return null;
        }
        throw err;
    }
}

async function waitForTableDeletion(tableName) {
    for (let attempt = 0; attempt < MAX_WAIT_ATTEMPTS; attempt++) {
        const status = await getTableStatus(tableName);
        if (!status) {
            return;
        }
        await sleep(WAIT_DELAY_MS);
    }
    throw new Error(`Timed out waiting for table deletion: ${tableName}`);
}

async function waitForTableStatus(tableName, expectedStatus) {
    for (let attempt = 0; attempt < MAX_WAIT_ATTEMPTS; attempt++) {
        const status = await getTableStatus(tableName);
        if (status === expectedStatus) {
            return;
        }
        await sleep(WAIT_DELAY_MS);
    }
    throw new Error(`Timed out waiting for table ${tableName} status ${expectedStatus}`);
}

function isResourceNotFound(err) {
    return err instanceof ResourceNotFoundException || err?.name === "ResourceNotFoundException";
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

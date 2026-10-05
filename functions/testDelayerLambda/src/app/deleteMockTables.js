"use strict";

const {
    DynamoDBClient,
    CreateTableCommand,
    DeleteTableCommand,
    DescribeTableCommand,
    ResourceNotFoundException
} = require("@aws-sdk/client-dynamodb");
const mockTableDefinitions = require("./mockTableDefinitions.json");

const ddbClient = new DynamoDBClient({});
const ACTIVE_STATUS = "ACTIVE";
const DELETING_STATUS = "DELETING";
const WAIT_DELAY_MS = Number(process.env.DELETE_MOCK_TABLES_WAIT_DELAY_MS || 1000);
const MAX_WAIT_ATTEMPTS = Number(process.env.DELETE_MOCK_TABLES_MAX_WAIT_ATTEMPTS || 120);

const buildTableDefinitions = () => mockTableDefinitions.map(definition =>
    JSON.parse(JSON.stringify(definition))
);

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

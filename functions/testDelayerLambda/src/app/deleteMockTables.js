"use strict";

const {
    DynamoDBClient,
    CreateTableCommand,
    DeleteTableCommand,
    EnableKinesisStreamingDestinationCommand,
    DescribeKinesisStreamingDestinationCommand,
    ResourceNotFoundException,
    waitUntilTableNotExists,
    waitUntilTableExists
} = require("@aws-sdk/client-dynamodb");
const mockTableDefinitions = require("./mockTableDefinitions.json");

const ddbClient = new DynamoDBClient({});
const ACTIVE_STATUS = "ACTIVE";
const TABLE_MAX_WAIT_TIME_SECONDS = Number(process.env.DELETE_MOCK_TABLES_MAX_WAIT_TIME_SECONDS || 60);
const TABLE_MIN_DELAY_SECONDS = Number(process.env.DELETE_MOCK_TABLES_MIN_DELAY_SECONDS || 2);
const KINESIS_WAIT_DELAY_MS = Number(process.env.DELETE_MOCK_TABLES_WAIT_DELAY_MS || 2000);
const KINESIS_MAX_WAIT_ATTEMPTS = Number(process.env.DELETE_MOCK_TABLES_MAX_WAIT_ATTEMPTS || 60);

const buildTableDefinitions = () => mockTableDefinitions.map(definition =>
    JSON.parse(JSON.stringify(definition))
);

exports.deleteMockTables = async () => {
    const tableDefinitions = buildTableDefinitions();
    const streamArn = process.env.CDC_KINESIS_STREAM_ARN;

    if (tableDefinitions.some(({ cdc }) => cdc) && !streamArn) {
        throw new Error("CDC_KINESIS_STREAM_ARN is required to recreate mock tables");
    }

    console.log(`Resetting ${tableDefinitions.length} mock tables`);
    await Promise.all(tableDefinitions.map(({ TableName }) => deleteTable(TableName)));

    console.log("All mock tables deleted; starting creation");
    for (const tableDefinition of tableDefinitions) {
        await createTable(tableDefinition, streamArn);
    }

    console.log(`Successfully recreated ${tableDefinitions.length} mock tables`);
    return {
        message: "Mock tables recreated",
        tables: tableDefinitions.map(({ TableName }) => TableName)
    };
};

async function deleteTable(tableName) {
    console.log(`Deleting mock table ${tableName}`);
    try {
        await ddbClient.send(new DeleteTableCommand({ TableName: tableName }));
    } catch (err) {
        if (isResourceNotFound(err)) {
            console.log(`Mock table ${tableName} does not exist; ready for creation`);
            return;
        }
        throw err;
    }

    console.log(`Waiting for mock table ${tableName} to be deleted`);
    await waitUntilTableNotExists(
      { client: ddbClient, maxWaitTime: TABLE_MAX_WAIT_TIME_SECONDS, minDelay: TABLE_MIN_DELAY_SECONDS },
      { TableName: tableName }
    );
    console.log(`Mock table ${tableName} deleted`);
}

async function createTable(tableDefinition, streamArn) {
    const { cdc, ...createTableInput } = tableDefinition;
    const tableName = tableDefinition.TableName;

    console.log(`Creating mock table ${tableName}`);
    await ddbClient.send(new CreateTableCommand(createTableInput));

    console.log(`Waiting for mock table ${tableName} to become ACTIVE`);
    await waitUntilTableExists(
      { client: ddbClient, maxWaitTime: TABLE_MAX_WAIT_TIME_SECONDS, minDelay: TABLE_MIN_DELAY_SECONDS },
      { TableName: tableName }
    );
    console.log(`Mock table ${tableName} is ACTIVE`);

    if (cdc) {
        console.log(`Enabling Kinesis streaming for mock table ${tableName} on ${streamArn}`);
        await ddbClient.send(new EnableKinesisStreamingDestinationCommand({ TableName: tableName, StreamArn: streamArn }));
        await waitForKinesisDestination(tableName, streamArn);
        console.log(`Kinesis streaming is ACTIVE for mock table ${tableName}`);
    }
}

async function waitForKinesisDestination(tableName, streamArn) {
    for (let attempt = 0; attempt < KINESIS_MAX_WAIT_ATTEMPTS; attempt++) {
        const result = await ddbClient.send(new DescribeKinesisStreamingDestinationCommand({ TableName: tableName }));
        const destination = result.KinesisDataStreamDestinations?.find(item => item.StreamArn === streamArn);
        if (destination?.DestinationStatus === ACTIVE_STATUS) {
            return;
        }
        if (destination?.DestinationStatus === "ENABLE_FAILED") {
            throw new Error(`Kinesis streaming destination failed for table: ${tableName}: ${destination.DestinationStatusDescription || "unknown reason"}`);
        }
        await sleep(KINESIS_WAIT_DELAY_MS);
    }
    throw new Error(`Timed out waiting for Kinesis streaming destination: ${tableName}`);
}

function isResourceNotFound(err) {
    return err instanceof ResourceNotFoundException || err?.name === "ResourceNotFoundException";
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

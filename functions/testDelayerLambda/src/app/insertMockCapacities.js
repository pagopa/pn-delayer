"use strict";
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");
const {
    DynamoDBDocumentClient,
    BatchWriteCommand
} = require("@aws-sdk/lib-dynamodb");
const csv = require("csv-parser");
const { Readable } = require("stream");

const s3Client = new S3Client({});
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient, {
                    marshallOptions: {
                      removeUndefinedValues: true,
                      convertEmptyValues: true
                    }
                  });
const lambdaClient = new LambdaClient({});

const TENDER_API_LAMBDA_ARN = process.env.PAPERCHANNELTENDERAPI_LAMBDA_ARN;

/**
 * INSERT_MOCK_CAPACITIES operation: downloads the CSV and writes rows to DynamoDB.
 * @param {Array<string>} params [fileName]
 * @returns {Promise<{message:string, processed:number}>}
 */
exports.insertMockCapacities = async (params = []) => {
    const BUCKET_NAME = process.env.BUCKET_NAME;
    let [paperDeliveryDriverCapacitiesTableName, fileName] = params;
    if (!paperDeliveryDriverCapacitiesTableName || !fileName) {
        throw new Error("Required parameter must be [paperDeliveryDriverCapacitiesTableName, fileName]");
    }

    if (!BUCKET_NAME) {
        throw new Error(
            "Environment variable BUCKET_NAME must be defined"
        );
    }

    const tenderId = await getActiveTender();
    if (!tenderId) {
        throw new Error("Unable to retrieve active tenderId");
    }

    const { Body } = await s3Client.send(
        new GetObjectCommand({ Bucket: BUCKET_NAME, Key: fileName })
    );

    // Ensure we have a Node.js Readable stream
    const stream = Body instanceof Readable ? Body : Readable.from(Body);

    let processed = 0;
    const itemsBuffer = [];

    for await (const record of stream.pipe(csv({ separator: ";" }))) {
        processed += 1;
        const driverCapacity = buildDriverCapacityRecord(record, tenderId);
        itemsBuffer.push(driverCapacity);
        if (itemsBuffer.length === 25) {
            await batchWriteItems(paperDeliveryDriverCapacitiesTableName, itemsBuffer.splice(0, itemsBuffer.length));
        }
    }
    if (itemsBuffer.length) {
        await batchWriteItems(paperDeliveryDriverCapacitiesTableName, itemsBuffer);
    }

    console.log("Processed data:", processed);
    return { message: "CSV imported successfully", processed };

 }


/**
 * Utility that performs a BatchWriteCommand and retries unprocessed items.
 * @param {Array<Object>} items
 */
async function batchWriteItems(paperDeliveryDriverCapacitiesTableName, items) {
    let unprocessed = items;
    do {
        const chunk = unprocessed.splice(0, 25);
        const command = new BatchWriteCommand({
            RequestItems: {
                [paperDeliveryDriverCapacitiesTableName]: chunk.map((Item) => ({
                    PutRequest: { Item }
                }))
            }
        });
        const response = await docClient.send(command);
        unprocessed = response.UnprocessedItems?.[paperDeliveryDriverCapacitiesTableName]?.map(
            (r) => r.PutRequest.Item
        ) || [];
        if (unprocessed.length) {
            // simple backoff
            await new Promise((r) => setTimeout(r, 200));
        }
    } while (unprocessed.length);
}

/**
 * Builds a driver capacity record from CSV row
 * @param {Object} record - CSV row object
 * @param {string} tenderId - Active tender ID
 * @returns {Object} DynamoDB item
 * @throws {Error} If required fields are missing or empty
 */
function buildDriverCapacityRecord(record, tenderId) {
    if (!record.unifiedDeliveryDriver || record.unifiedDeliveryDriver.trim() === '') {
        throw new Error('Field unifiedDeliveryDriver is required and cannot be empty');
    }
    if (!record.geoKey || record.geoKey.trim() === '') {
        throw new Error('Field geoKey is required and cannot be empty');
    }
    if (!record.capacity || record.capacity.trim() === '') {
        throw new Error('Field capacity is required and cannot be empty');
    }

    const mappedRecord = {
        pk: `${tenderId}~${record.unifiedDeliveryDriver}~${record.geoKey}`,
        activationDateFrom: record.activationDateFrom === ''
              ? new Date().toISOString()
              : record.activationDateFrom,
        activationDateTo: record.activationDateTo === '' ? undefined : record.activationDateTo,
        capacity: parseInt(record.capacity, 10),
        createdAt: new Date().toISOString(),
        geoKey: record.geoKey,
        peakCapacity: parseInt(record.peakCapacity, 10),
        products: record.products === '' ? undefined : record.products.split(',').map((product) => product.trim()),
        tenderId: tenderId,
        tenderIdGeoKey: `${tenderId}~${record.geoKey}`,
        unifiedDeliveryDriver: record.unifiedDeliveryDriver
    };

    return mappedRecord;
}

/**
 * Invoke TenderApiLambda to get tender information
 * @returns {Promise<string>} Active tender ID
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
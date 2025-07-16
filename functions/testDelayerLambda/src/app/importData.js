"use strict";
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    BatchWriteCommand
} = require("@aws-sdk/lib-dynamodb");
const csv = require("csv-parser");
const { Readable } = require("stream");
const TABLE_NAME = "pn-DelayerPaperDelivery";

// AWS SDK clients
const s3Client = new S3Client({});
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);


/**
 * IMPORT_DATA operation: downloads the CSV and writes rows to DynamoDB.
 * @param {Array<string>} _params – future use (per ora ignorati)
 * @returns {Promise<{message:string, processed:number}>}
 */
exports.importData = async (_params = []) => {
    const BUCKET_NAME = process.env.BUCKET_NAME;
    const OBJECT_KEY = process.env.OBJECT_KEY;

    if (!BUCKET_NAME || !OBJECT_KEY) {
        throw new Error(
            "Environment variables BUCKET_NAME and OBJECT_KEY must be defined"
        );
}

    const { Body } = await s3Client.send(
        new GetObjectCommand({ Bucket: BUCKET_NAME, Key: OBJECT_KEY })
    );

    // Ensure we have a Node.js Readable stream
    const stream = Body instanceof Readable ? Body : Readable.from(Body);

    let processed = 0;
    const itemsBuffer = [];
    for await (const record of stream.pipe(csv({ separator: ";" }))) {
        processed += 1;
        itemsBuffer.push(record);
        if (itemsBuffer.length === 25) {
            await batchWriteItems(itemsBuffer.splice(0, itemsBuffer.length));
        }
    }
    if (itemsBuffer.length) {
        await batchWriteItems(itemsBuffer);
    }

    console.log("Processed data:", processed);
    return { message: "CSV imported successfully", processed };
}

/**
 * Utility that performs a BatchWriteCommand and retries unprocessed items.
 * @param {Array<Object>} items
 */
async function batchWriteItems(items) {
    let unprocessed = items;
    do {
        const chunk = unprocessed.splice(0, 25);
        const command = new BatchWriteCommand({
            RequestItems: {
                [TABLE_NAME]: chunk.map((Item) => ({
                    PutRequest: { Item }
                }))
            }
        });
        const response = await docClient.send(command);
        unprocessed = response.UnprocessedItems?.[TABLE_NAME]?.map(
            (r) => r.PutRequest.Item
        ) || [];
        if (unprocessed.length) {
            // simple backoff
            await new Promise((r) => setTimeout(r, 200));
        }
    } while (unprocessed.length);
}
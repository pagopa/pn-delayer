"use strict";
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    QueryCommand,
    BatchWriteCommand
} = require("@aws-sdk/lib-dynamodb");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { LocalDate, DayOfWeek, TemporalAdjusters } = require("@js-joda/core");

const PAPER_DELIVERY_TABLE_NAME = "pn-DelayerPaperDelivery";
const USED_CAPACITY_TABLE_NAME = "pn-PaperDeliveryUsedCapacities";
const USED_SENDER_LIMIT_TABLE_NAME = "pn-PaperDeliveryUsedSenderLimit";
const COUNTERS_TABLE_NAME = "pn-PaperDeliveryCounters";

const s3Client = new S3Client({});
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * DELETE_DATA operation: downloads the CSV and delete rows to DynamoDB.
 * @param {Array<string>} params[fileName]
 * @returns {Promise<{message:string, processed:number}>}
 */
exports.deleteData = async (params = []) => {
    const BUCKET_NAME = process.env.BUCKET_NAME;
    let OBJECT_KEY = process.env.OBJECT_KEY;
    const [fileName] = params;

    if (fileName) {
        OBJECT_KEY = fileName;
    }

    if (!BUCKET_NAME || !OBJECT_KEY) {
        throw new Error(
            "Environment variables BUCKET_NAME and OBJECT_KEY must be defined"
        );
    }

    let processed = 0;
    let entitiesBuffer = [];
    let allEntities = [];

    try {
        const { Body } = await s3Client.send(
            new GetObjectCommand({ Bucket: BUCKET_NAME, Key: OBJECT_KEY })
        );

        const stream = Body instanceof Readable ? Body : Readable.from(Body);

        for await (const record of stream.pipe(csv({ separator: ";" }))) {
            const requestId = record.requestId;
            if (!requestId) continue;

            const queryInput = {
                TableName: PAPER_DELIVERY_TABLE_NAME,
                IndexName: "requestId-CreatedAt-index",
                KeyConditionExpression: "requestId = :id",
                ExpressionAttributeValues: { ":id": requestId }
            };

            let queryResult;
            try {
                queryResult = await docClient.send(new QueryCommand(queryInput));
            } catch (err) {
                console.error("Errore QueryCommand:", err, queryInput);
                continue;
            }

            if (queryResult.Items && queryResult.Items.length > 0) {
                allEntities.push(...queryResult.Items);
            }
        }

        await batchDeleteEntities(allEntities);
        const grouped = groupRecordsByProductAndProvince(allEntities);
        const deliveryWeek = getDeliveryWeek();
        await batchDeleteCounters(grouped, deliveryWeek);
        await batchDeleteUsedSenderLimit(allEntities, deliveryWeek);
        const printCapacitiesEntities = allEntities.filter(e => e.sk.endsWith('EVALUATE_PRINT_CAPACITY'));
        await batchDeleteUsedCapacity(printCapacitiesEntities, deliveryWeek);

        console.log("Processed deletions:", processed);
        return { message: "Delete completed", processed };
    } catch (err) {
        console.error("Errore deleteData:", err);
        throw err;
    }
};

async function batchDeleteEntities(entities) {
    const deleteRequests = entities.map(item => ({
        Key: {
            pk: item.pk,
            sk: item.sk
            }
        }));
    await batchDeleteItems(deleteRequests, PAPER_DELIVERY_TABLE_NAME);
    console.log(`Deleted ${deleteRequests.length} entities from table pn-DelayerPaperDelivery`);
}

async function batchDeleteCounters(excludeGroupedRecords, deliveryWeek) {
    const deleteRequests = Object.keys(excludeGroupedRecords).map(key => ({
            Key: {
                pk: deliveryWeek,
                sk: `EXCLUDE~${key}`
            }
    }));
    await batchDeleteItems(deleteRequests, COUNTERS_TABLE_NAME);
    console.log(`Deleted ${deleteRequests.length} items from table pn-PaperDeliveryCounters`);
}

async function batchDeleteUsedSenderLimit(entities, deliveryWeek) {
    const grouped = groupRecordsBySenderProductProvince(entities);

    const deleteRequests = Object.keys(grouped).map(key => ({
        DeleteRequest: {
            Key: {
                pk: key,
                sk: deliveryWeek
            }
        }
    }));
    await batchDeleteItems(deleteRequests, USED_SENDER_LIMIT_TABLE_NAME);
    console.log(`Deleted ${deleteRequests.length} items from table pn-PaperDeliveryUsedSenderLimit`);
}

async function batchDeleteUsedCapacity(entities, deliveryWeek) {
    const grouped = groupRecordsByDriverIdProvinceCap(entities);

    const uniqueKeys = getUniqueKeysForDeletion(grouped);
    const deleteRequests = Object.keys(uniqueKeys).map(key => ({
            DeleteRequest: {
                Key: {
                    pk: key,
                    sk: deliveryWeek
                }
            }
        }));
        await batchDeleteItems(deleteRequests, USED_CAPACITY_TABLE_NAME);
        console.log(`Deleted ${deleteRequests.length} items from table pn-PaperDeliveryUsedCapacities`);
}


async function batchDeleteItems(deleteRequests, tableName) {
    let unprocessed = deleteRequests;
    do {
        const chunk = unprocessed.splice(0, 25);
        const command = new BatchWriteCommand({
            RequestItems: {
                [tableName]: chunk.map((Item) => ({
                    DeleteRequest: { Item }
                }))
            }
        });
        const response = await docClient.send(command);
        unprocessed = response.UnprocessedItems?.[tableName]?.map(
            (r) => r.DeleteRequest.Item
        ) || [];
        if (unprocessed.length) {
            await new Promise((r) => setTimeout(r, 200));
        }
    } while (unprocessed.length);
}

function getDeliveryWeek() {
    const dayOfWeek = 1;
    return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
}

const groupRecordsByProductAndProvince = (records) => {
  return records.reduce((acc, record) => {
    const key = `${record.province}~${record.productType}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

function groupRecordsBySenderProductProvince(records) {
    return records.reduce((acc, record) => {
        const key = `${record.senderPaId}~${record.productType}~${record.province}`;
        if (!acc[key]) {
            acc[key] = [];
        }
        acc[key].push(record);
        return acc;
    }, {});
}

function groupRecordsByDriverIdProvinceCap(records) {
    return records.reduce((acc, record) => {
        const key = `${record.unifiedDeliveryDriver}~${record.province}~${record.cap}`;
        if (!acc[key]) {
            acc[key] = [];
        }
        acc[key].push(record);
        return acc;
    }, {});
}

function getUniqueKeysForDeletion(groupedRecords) {
    const uniqueKeys = new Set();

    Object.keys(groupedRecords).forEach(key => {
        const [driver, province, cap] = key.split('~');
        uniqueKeys.add(`${driver}~${province}`);
        uniqueKeys.add(`${driver}~${cap}`);
    });

    return uniqueKeys;
}
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

const s3Client = new S3Client({});
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

// Configurazione parallelismo
const CONCURRENT_QUERIES = 10; // Query parallele
const BATCH_SIZE = 25; // Dimensione batch DynamoDB (max)
const BATCH_DELAY = 50; // Ridotto da 200ms a 50ms
const CONCURRENT_BATCHES = 5; // Numero di batch paralleli

/**
* DELETE_DATA operation: downloads the CSV and delete rows to DynamoDB.
* @param {Array<string>} params[fileName]
* @param {Array<string>} params - Ordered array in the form
* [paperDeliveryTableName, deliveryDriverUsedCapacitiesTableName,senderUsedLimitTableName, countersTableName, fileName?]
* @returns {Promise<{message:string, processed:number}>}
*/
exports.deleteData = async (params = []) => {
    const BUCKET_NAME = process.env.BUCKET_NAME;
    let OBJECT_KEY = process.env.OBJECT_KEY;
    let [paperDeliveryTableName, deliveryDriverUsedCapacitiesTableName, senderUsedLimitTableName, countersTableName, fileName] = params;

    if (!paperDeliveryTableName || !deliveryDriverUsedCapacitiesTableName || !senderUsedLimitTableName || !countersTableName) {
        throw new Error("Required parameters must be [paperDeliveryTableName, " +
            "deliveryDriverUsedCapacitiesTableName, senderUsedLimitTableName, countersTableName]");
    }
    if (fileName) {
        OBJECT_KEY = fileName;
    }

    if (!BUCKET_NAME || !OBJECT_KEY) {
        throw new Error(
            "Environment variables BUCKET_NAME and OBJECT_KEY must be defined"
        );
    }

    let requestIds = [];

    try {
        const { Body } = await s3Client.send(
            new GetObjectCommand({ Bucket: BUCKET_NAME, Key: OBJECT_KEY })
        );

        const stream = Body instanceof Readable ? Body : Readable.from(Body);

        // Prima fase: raccogliere tutti i requestId
        for await (const record of stream.pipe(csv({ separator: ";" }))) {
            const requestId = record.requestId;
            if (requestId) {
                requestIds.push(requestId);
            }
        }

        console.log(`Found ${requestIds.length} requestIds to process`);

        // Seconda fase: query parallele in chunk
        const allEntities = await queryEntitiesInParallel(
            paperDeliveryTableName,
            requestIds,
            CONCURRENT_QUERIES
        );

        console.log(`Retrieved ${allEntities.length} entities from DynamoDB`);

        if (allEntities.length > 0) {

            const entitiesByWeek = allEntities.reduce((acc, e) => {
                const week = e.pk.split('~')[0];
                if (!acc[week]) acc[week] = [];
                acc[week].push(e);
                return acc;
            }, {});

            console.log(`Found ${Object.keys(entitiesByWeek).length} distinct deliveryWeek(s): ${Object.keys(entitiesByWeek).join(', ')}`);

            // Terza fase: delete parallele
            await Promise.all([
                // PAPER DELIVERY
                batchDeleteGeneric(
                    paperDeliveryTableName,
                    allEntities.map(e => ({ pk: e.pk, sk: e.sk })),
                    k => ({ pk: k.pk, sk: k.sk })
                ),

               (async () => {
                   for (const [deliveryWeek, weekEntities] of Object.entries(entitiesByWeek)) {

                       const previousDeliveryWeek = LocalDate.parse(deliveryWeek)
                           .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                           .minusWeeks(1)
                           .toString();

                       console.log(`Processing deliveryWeek=${deliveryWeek}, previousDeliveryWeek=${previousDeliveryWeek}, entities=${weekEntities.length}`);

                       await Promise.all([
                           // COUNTERS
                           (async () => {
                               const grouped = groupRecordsByProductAndProvince(weekEntities);
                               const keys = Object.keys(grouped).map(k => ({
                                   pk: deliveryWeek,
                                   sk: `EXCLUDE~${k}`
                               }));
                               keys.push({ pk: "PRINT", sk: deliveryWeek });
                               await batchDeleteGeneric( countersTableName, keys, k => ({ pk: k.pk, sk: k.sk }) );
                           })(),

                           // SENDER LIMIT
                           (async () => {
                               const grouped = groupRecordsBySenderProductProvince(weekEntities);
                               const keys = Object.keys(grouped).map(k => ({
                                   pk: k,
                                   sk: previousDeliveryWeek
                               }));

                               await batchDeleteGeneric(senderUsedLimitTableName, keys, k => ({ pk: k.pk, deliveryDate: k.sk }) );
                           })(),

                           // CAPACITY
                           (async () => {
                               const printCapacitiesEntities = weekEntities.filter(e =>
                                   e.pk.endsWith("EVALUATE_PRINT_CAPACITY")
                               );

                               const grouped = groupRecordsByDriverIdProvinceCap(printCapacitiesEntities);
                               const uniqueKeys = getUniqueKeysForDeletion(grouped);

                               const keys = Array.from(uniqueKeys).map(pk => ({
                                   pk,
                                   sk: deliveryWeek
                               }));

                               await batchDeleteGeneric(deliveryDriverUsedCapacitiesTableName,keys,k => ({unifiedDeliveryDriverGeokey: k.pk,deliveryDate: k.sk })
                               );
                           })()
                       ]);
                   }
               })()
            ]);
        }
        console.log("Processed deletions:", allEntities.length);
        return { message: "Delete completed", processed: allEntities.length };
    } catch (err) {
        console.error("Errore deleteData:", err);
        throw err;
    }
};

/**
* Delete entities from DynamoDB in parallel with concurrency control and retry handling.
* Splits the provided keys into batches, performs batched delete operations,
* and retries unprocessed items up to a fixed number of attempts, waiting between retries.
*/
async function batchDeleteGeneric(tableName, keys, keyBuilder) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);

    const chunks = [];
    for (let i = 0; i < pending.length; i += BATCH_SIZE) {
      chunks.push(pending.slice(i, i + BATCH_SIZE));
    }

    for (let i = 0; i < chunks.length; i += CONCURRENT_BATCHES) {
        const batchPromises = chunks.slice(i, i + CONCURRENT_BATCHES).map(async (initialChunk) => {
            let chunk = initialChunk.map(k => ({
                DeleteRequest: { Key: keyBuilder(k) }
            }));
            let retries = 3;
            while (retries > 0) {
                const res = await docClient.send(new BatchWriteCommand({
                    RequestItems: { [tableName]: chunk }
                }));
                const notProcessed = res.UnprocessedItems?.[tableName] || [];
                if (!notProcessed.length) return;
                chunk = notProcessed;
                retries--;
                await sleep(BATCH_DELAY);
            }
            if (chunk.length) {
                console.warn(`Unprocessed items after retries: ${chunk.length}`);
            }
        });

        await Promise.all(batchPromises);

      if (i + CONCURRENT_BATCHES < chunks.length) {
        await sleep(BATCH_DELAY);
      }
    }
}

/**
* QUERY
*/
async function queryEntitiesInParallel(tableName, requestIds, concurrency) {
    const all = [];

    for (let i = 0; i < requestIds.length; i += concurrency) {
        const chunk = requestIds.slice(i, i + concurrency);

        const results = await Promise.all(
            chunk.map(id => queryEntitiesByRequestId(tableName, id))
        );

        results.forEach(r => all.push(...r));
    }

    return all;
}

async function queryEntitiesByRequestId(tableName, requestId) {
    try {
        const res = await docClient.send(new QueryCommand({
            TableName: tableName,
            IndexName: "requestId-CreatedAt-index",
            KeyConditionExpression: "requestId = :id",
            ExpressionAttributeValues: { ":id": requestId }
        }));

        return res.Items || [];
    } catch (err) {
        console.error(`Query error ${requestId}`, err);
        return [];
    }
}

/**
* UTILS
*/
const sleep = ms => new Promise(r => setTimeout(r, ms));

const groupRecordsByProductAndProvince = records =>
    records.reduce((acc, r) => {
        const key = `${r.province}~${r.productType}`;
        (acc[key] ||= []).push(r);
        return acc;
    }, {});

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
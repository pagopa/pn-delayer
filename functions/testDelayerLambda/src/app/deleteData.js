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

        if(allEntities.length > 0){
            // Terza fase: delete parallele
            const deliveryWeek = allEntities[0].pk.split('~')[0];
            const previousDeliveryWeek = LocalDate.parse(deliveryWeek).with(TemporalAdjusters.previousOrSame(DayOfWeek.of(1))).minusWeeks(1).toString();
            await Promise.all([
                batchDeleteEntities(paperDeliveryTableName, allEntities),
                (async () => {
                    const grouped = groupRecordsByProductAndProvince(allEntities);
                    await batchDeleteCounters(countersTableName, grouped, deliveryWeek);
                })(),
                batchDeleteUsedSenderLimit(senderUsedLimitTableName, allEntities, previousDeliveryWeek),
                (async () => {
                    const printCapacitiesEntities = allEntities.filter(e => e.pk.endsWith('EVALUATE_PRINT_CAPACITY'));
                    await batchDeleteUsedCapacity(deliveryDriverUsedCapacitiesTableName, printCapacitiesEntities, deliveryWeek);
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
 * Query entities in parallel with concurrency control
 */
async function queryEntitiesInParallel(tableName, requestIds, concurrency) {
    const allEntities = [];
    const chunks = [];

    // Dividi in chunk per il parallelismo
    for (let i = 0; i < requestIds.length; i += concurrency) {
        chunks.push(requestIds.slice(i, i + concurrency));
    }

    for (const chunk of chunks) {
        const promises = chunk.map(requestId =>
            queryEntitiesByRequestId(tableName, requestId)
        );

        const results = await Promise.all(promises);
        results.forEach(entities => allEntities.push(...entities));

    }

    return allEntities;
}

/**
 * Query entities by single requestId
 */
async function queryEntitiesByRequestId(tableName, requestId) {
    const queryInput = {
        TableName: tableName,
        IndexName: "requestId-CreatedAt-index",
        KeyConditionExpression: "requestId = :id",
        ExpressionAttributeValues: { ":id": requestId }
    };

    try {
        const queryResult = await docClient.send(new QueryCommand(queryInput));
        return queryResult.Items || [];
    } catch (err) {
        console.error(`Error querying requestId ${requestId}:`, err);
        return [];
    }
}

async function batchDeleteEntities(paperDeliveryTableName, entities) {
    const keys = entities.map(e => ({ pk: e.pk, sk: e.sk }));
    await batchDeleteItems(keys, paperDeliveryTableName);
    console.log(`Deleted ${keys.length} entities from table ${paperDeliveryTableName}`);
  }

  async function batchDeleteCounters(countersTableName, excludeGroupedRecords, deliveryWeek) {
    const keys = Object.keys(excludeGroupedRecords).map(k => ({
      pk: deliveryWeek,
      sk: `EXCLUDE~${k}`,
    }));
    keys.push({ pk: 'PRINT', sk: deliveryWeek });
    await batchDeleteItems(keys, countersTableName);
    console.log(`Deleted ${keys.length} items from table ${countersTableName}`);
  }

  async function batchDeleteUsedSenderLimit(senderUsedLimitTableName, entities, previousDeliveryWeek) {
    const grouped = groupRecordsBySenderProductProvince(entities);
    const keys = Object.keys(grouped).map(k => ({ pk: k, sk: previousDeliveryWeek }));
    await batchDeleteUsedSenderLimitItems(keys, senderUsedLimitTableName);
    console.log(`Deleted ${keys.length} items from table ${senderUsedLimitTableName}`);
  }

  async function batchDeleteUsedCapacity(deliveryDriverUsedCapacitiesTableName, entities, deliveryWeek) {
    const grouped = groupRecordsByDriverIdProvinceCap(entities);
    const uniqueKeys = getUniqueKeysForDeletion(grouped);
    const keys = Array.from(uniqueKeys).map(pk => ({ pk: pk, sk: deliveryWeek }));
    await batchDeleteUsedCapacityItems(keys, deliveryDriverUsedCapacitiesTableName);
    console.log(`Deleted ${keys.length} items from table ${deliveryDriverUsedCapacitiesTableName}`);
  }


  async function batchDeleteItems(keys, tableName) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);

    const chunks = [];
    for (let i = 0; i < pending.length; i += BATCH_SIZE) {
      chunks.push(pending.slice(i, i + BATCH_SIZE));
    }

    // Elabora i chunk in parallelo (max 5 alla volta per evitare throttling)
    for (let i = 0; i < chunks.length; i += CONCURRENT_BATCHES) {
      const batchPromises = chunks.slice(i, i + CONCURRENT_BATCHES).map(async chunk => {
        let retries = 3;
        while (retries > 0) {
          const command = new BatchWriteCommand({
            RequestItems: {
              [tableName]: chunk.map(k => ({
                DeleteRequest: { Key: { pk: k.pk, sk: k.sk } }
              }))
            }
          });

          const res = await docClient.send(command);
          const notProcessed = res.UnprocessedItems?.[tableName] || [];

          if (notProcessed.length === 0) break;

          retries--;
          chunk = notProcessed.map(r => r.DeleteRequest.Key);
          await new Promise(r => setTimeout(r, BATCH_DELAY));
        }
      });

      await Promise.all(batchPromises);

      if (i + CONCURRENT_BATCHES < chunks.length) {
        await new Promise(r => setTimeout(r, BATCH_DELAY));
      }
    }
  }

  async function batchDeleteUsedSenderLimitItems(keys, tableName) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);

    const chunks = [];
    for (let i = 0; i < pending.length; i += BATCH_SIZE) {
      chunks.push(pending.slice(i, i + BATCH_SIZE));
    }

    for (let i = 0; i < chunks.length; i += CONCURRENT_BATCHES) {
      const batchPromises = chunks.slice(i, i + CONCURRENT_BATCHES).map(async chunk => {
        let retries = 3;
        while (retries > 0) {
          const command = new BatchWriteCommand({
            RequestItems: {
              [tableName]: chunk.map(k => ({
                DeleteRequest: { Key: { pk: k.pk, deliveryDate: k.sk } }
              }))
            }
          });

          const res = await docClient.send(command);
          const notProcessed = res.UnprocessedItems?.[tableName] || [];

          if (notProcessed.length === 0) break;

          retries--;
          chunk = notProcessed.map(r => r.DeleteRequest.Key);
          await new Promise(r => setTimeout(r, BATCH_DELAY));
        }
      });

      await Promise.all(batchPromises);

      if (i + CONCURRENT_BATCHES < chunks.length) {
        await new Promise(r => setTimeout(r, BATCH_DELAY));
      }
    }
  }

  async function batchDeleteUsedCapacityItems(keys, tableName) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);

    const chunks = [];
    for (let i = 0; i < pending.length; i += BATCH_SIZE) {
      chunks.push(pending.slice(i, i + BATCH_SIZE));
    }

    for (let i = 0; i < chunks.length; i += CONCURRENT_BATCHES) {
      const batchPromises = chunks.slice(i, i + CONCURRENT_BATCHES).map(async chunk => {
        let retries = 3;
        while (retries > 0) {
          const command = new BatchWriteCommand({
            RequestItems: {
              [tableName]: chunk.map(k => ({
                DeleteRequest: { Key: { unifiedDeliveryDriverGeokey: k.pk, deliveryDate: k.sk } }
              }))
            }
          });

          const res = await docClient.send(command);
          const notProcessed = res.UnprocessedItems?.[tableName] || [];

          if (notProcessed.length === 0) break;

          retries--;
          chunk = notProcessed.map(r => r.DeleteRequest.Key);
          await new Promise(r => setTimeout(r, BATCH_DELAY));
        }
      });

      await Promise.all(batchPromises);

      if (i + CONCURRENT_BATCHES < chunks.length) {
        await new Promise(r => setTimeout(r, BATCH_DELAY));
      }
    }
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
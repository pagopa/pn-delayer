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
const USED_CAPACITY_TABLE_NAME = "pn-PaperDeliveryDriverUsedCapacities";
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

        if(allEntities.length > 0){
            await batchDeleteEntities(allEntities);
            const grouped = groupRecordsByProductAndProvince(allEntities);
            const deliveryWeek = getDeliveryWeek();
            await batchDeleteCounters(grouped, deliveryWeek);
            await batchDeleteUsedSenderLimit(allEntities);
            const printCapacitiesEntities = allEntities.filter(e => e.pk.endsWith('EVALUATE_PRINT_CAPACITY'));
            await batchDeleteUsedCapacity(printCapacitiesEntities, deliveryWeek);
        }

        console.log("Processed deletions:", processed);
        return { message: "Delete completed", processed };
    } catch (err) {
        console.error("Errore deleteData:", err);
        throw err;
    }
};

async function batchDeleteEntities(entities) {
    const keys = entities.map(e => ({ pk: e.pk, sk: e.sk }));
    await batchDeleteItems(keys, PAPER_DELIVERY_TABLE_NAME);
    console.log(`Deleted ${keys.length} entities from table ${PAPER_DELIVERY_TABLE_NAME}`);
  }

  async function batchDeleteCounters(excludeGroupedRecords, deliveryWeek) {
    const keys = Object.keys(excludeGroupedRecords).map(k => ({
      pk: deliveryWeek,
      sk: `EXCLUDE~${k}`,
    }));
    await batchDeleteItems(keys, COUNTERS_TABLE_NAME);
    console.log(`Deleted ${keys.length} items from table ${COUNTERS_TABLE_NAME}`);
  }

  async function batchDeleteUsedSenderLimit(entities) {
    const week = getPreviousWeek();
    const grouped = groupRecordsBySenderProductProvince(entities);
    const keys = Object.keys(grouped).map(k => ({ pk: k, sk: week }));
    await batchDeleteUsedSenderLimitItems(keys, USED_SENDER_LIMIT_TABLE_NAME);
    console.log(`Deleted ${keys.length} items from table ${USED_SENDER_LIMIT_TABLE_NAME}`);
  }

  async function batchDeleteUsedCapacity(entities, deliveryWeek) {
    const grouped = groupRecordsByDriverIdProvinceCap(entities);
    const uniqueKeys = getUniqueKeysForDeletion(grouped);
    const keys = Array.from(uniqueKeys).map(pk => ({ pk: pk, sk: deliveryWeek }));
    await batchDeleteUsedCapacityItems(keys, USED_CAPACITY_TABLE_NAME);
    console.log(`Deleted ${keys.length} items from table ${USED_CAPACITY_TABLE_NAME}`);
  }


  async function batchDeleteItems(keys, tableName) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);
    do {
      const chunk = pending.splice(0, 25);

      const command = new BatchWriteCommand({
        RequestItems: {
          [tableName]: chunk.map(k => ({
            DeleteRequest: { Key: { pk: k.pk, sk: k.sk } }
          }))
        }
      });

      const res = await docClient.send(command);
      const notProcessed = res.UnprocessedItems?.[tableName]?.map(r => r.DeleteRequest.Key) || [];
      pending.push(...notProcessed);

      if (pending.length) {
        await new Promise(r => setTimeout(r, 200));
      }
    } while (pending.length);
  }

  async function batchDeleteUsedSenderLimitItems(keys, tableName) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);
    do {
      const chunk = pending.splice(0, 25);

      const command = new BatchWriteCommand({
        RequestItems: {
          [tableName]: chunk.map(k => ({
            DeleteRequest: { Key: { pk: k.pk, deliveryDate: k.sk } }
          }))
        }
      });

      const res = await docClient.send(command);
      const notProcessed = res.UnprocessedItems?.[tableName]?.map(r => r.DeleteRequest.Key) || [];
      pending.push(...notProcessed);

      if (pending.length) {
        await new Promise(r => setTimeout(r, 200));
      }
    } while (pending.length);
  }

  async function batchDeleteUsedCapacityItems(keys, tableName) {
    if (!keys?.length) return;
    let pending = keys.filter(k => k?.pk && k?.sk);
    do {
      const chunk = pending.splice(0, 25);

      const command = new BatchWriteCommand({
        RequestItems: {
          [tableName]: chunk.map(k => ({
            DeleteRequest: { Key: { unifiedDeliveryDriverGeokey: k.pk, deliveryDate: k.sk } }
          }))
        }
      });

      const res = await docClient.send(command);
      const notProcessed = res.UnprocessedItems?.[tableName]?.map(r => r.DeleteRequest.Key) || [];
      pending.push(...notProcessed);

      if (pending.length) {
        await new Promise(r => setTimeout(r, 200));
      }
    } while (pending.length);
  }


function getDeliveryWeek() {
    const dayOfWeek = 1;
    return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
}

function getPreviousWeek() {
  const dayOfWeek = 1;
  return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).minusWeeks(1).toString();
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
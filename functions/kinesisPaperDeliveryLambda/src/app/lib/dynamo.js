const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  BatchWriteCommand,
  BatchGetCommand,
  UpdateCommand,
  DynamoDBDocumentClient
} = require("@aws-sdk/lib-dynamodb");
const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const counterTableName = process.env.KINESIS_PAPERDELIVERY_COUNTERTABLE;
const { LocalDate, DayOfWeek, TemporalAdjusters } = require('@js-joda/core');

function calculateTtl(){
  const ttlDays = parseInt(process.env.KINESIS_PAPERDELIVERY_COUNTERTTLDAYS, 10) || 14;
  const expireDate = new Date();
  expireDate.setDate(expireDate.getDate() + ttlDays);
  return Math.floor(expireDate.getTime() / 1000);
}

function retrieveCounterMap(excludeGroupedRecords) {
  const result = {};
  for (const key of Object.keys(excludeGroupedRecords)) {
    const records = excludeGroupedRecords[key];
    const productTypeKey = key.split("~")[1];

    let filteredRecords;

    if (productTypeKey === "RS") {
      filteredRecords = records;
    } else {
      filteredRecords = records.filter(
        record => record.entity.attempt && parseInt(record.entity.attempt, 10) === 1
      );
    }

    if (filteredRecords.length > 0) {
      result[key] = filteredRecords.length;
    }
  }
  return result;
}

function getDeliveryWeek() {
  const dayOfWeek = parseInt(process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK, 10) || 1;
  return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
}

async function updateExcludeCounter(excludeGroupedRecords, batchItemFailures) {
   
    const deliveryDate = getDeliveryWeek();
    let ttl = calculateTtl();
    let counterMap = retrieveCounterMap(excludeGroupedRecords);

    for (const [productTypeProvince, inc] of Object.entries(counterMap)) {
      const sk = `EXCLUDE~${productTypeProvince}`;
      try {
        const input = {
            TableName: counterTableName,
            Key: {
              pk: deliveryDate,
              sk: sk
            },
            UpdateExpression: 'ADD #counter :inc SET #ttl = :ttl',
            ExpressionAttributeNames: {
            '#counter': 'counter',
            '#ttl': 'ttl'
            },
            ExpressionAttributeValues: {
            ':inc': inc,
            ':ttl': ttl
            }
          };
          const command = new UpdateCommand(input);
          await docClient.send(command);
          console.log(`updateSuccessfully for ${sk}`);
      } catch (error) {
        console.error(`Failed to update counter for sk: ${sk}`, error);
        const failedRecords = excludeGroupedRecords[productTypeProvince];
        if (failedRecords) {
          const failedSeqNumbers = failedRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; });
          batchItemFailures.push(...failedSeqNumbers);
        }
      }
    }
    return batchItemFailures;
}

async function batchWritePaperDeliveryRecords(paperDeliveryRecords, batchItemFailures) {
  const batch_size = process.env.KINESIS_BATCHSIZE;
  console.log(`Batch size: ${batch_size}`);
  const tableName = process.env.KINESIS_PAPERDELIVERY_TABLE;

  const params = {
        RequestItems: {
          [tableName]: paperDeliveryRecords.map(record => ({
            PutRequest: {
              Item: record.entity
            }
          }))
        }
  }

  try {
    const command = new BatchWriteCommand(params);
    const response = await docClient.send(command);
    console.log(`Batch write successful for ${paperDeliveryRecords.length} items.`);

    const writeRequests = response.UnprocessedItems[tableName];
    if (writeRequests) {
      const failedIDs = [];
      console.log(`Unprocessed items: ${writeRequests.length}`);
      for (const writeRequest of writeRequests) {
        const unprocessedEntity = writeRequest.PutRequest.Item;
        const failedRecord = paperDeliveryRecords.find(record => record.entity.sk === unprocessedEntity.sk.S);
        if (failedRecord) {
          failedIDs.push(failedRecord.kinesisSeqNumber);
        }
      }
      batchItemFailures = batchItemFailures.concat(failedIDs.map((i) => {
        return { itemIdentifier: i };
      }));
      console.warn("batchItemFailures:" + JSON.stringify(batchItemFailures));
    }
  } catch (error) {
    console.error('Error in batch write:', error);
    batchItemFailures = batchItemFailures.concat(paperDeliveryRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; }));
  }
  return batchItemFailures;
}

async function batchWriteKinesisEventRecords(eventRecords) {
  const tableName = process.env.KINESIS_PAPERDELIVERY_EVENTTABLE;
  const params = {
    RequestItems: {
      [tableName]: eventRecords.map(record => ({
        PutRequest: { Item: record }
      }))
    }
  };
  const command = new BatchWriteCommand(params);
  return await docClient.send(command);
}

async function batchGetKinesisEventRecords(keys) {
  const tableName = process.env.KINESIS_PAPERDELIVERY_EVENTTABLE;
  const params = {
    RequestItems: {
      [tableName]: {
        Keys: keys.map(key => (
            {
              requestId: key
            }
        ))
    }
    }
  };
  const command = new BatchGetCommand(params);
  return await docClient.send(command).then(response => {
    const items = response.Responses[tableName];
    if (!items || items.length === 0) {
      return [];
    }
    return items.map(item => item.requestId);
  });
}

module.exports = { batchWritePaperDeliveryRecords,
                   updateExcludeCounter,
                   batchWriteKinesisEventRecords,
                   batchGetKinesisEventRecords };
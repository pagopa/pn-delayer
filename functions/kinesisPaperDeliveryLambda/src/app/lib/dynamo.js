const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  BatchWriteCommand,
  BatchGetCommand,
  UpdateCommand,
  DynamoDBDocumentClient
} = require("@aws-sdk/lib-dynamodb");
const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const counterTableName = process.env.PAPER_DELIVERY_COUNTER_TABLE_NAME;
const { LocalDate, DayOfWeek, TemporalAdjusters } = require('@js-joda/core');

function calculateTtl(){
  const ttlDays = parseInt(process.env.KINESIS_PAPER_DELIVERY_COUNTER_TTL_DAYS, 10) || 14;
  const expireDate = new Date();
  expireDate.setDate(expireDate.getDate() + ttlDays);
  return Math.floor(expireDate.getTime() / 1000);
}

function retrieveCounterMap(excludeGroupedRecords) {
  return Object.keys(excludeGroupedRecords).map(key => {
    const value = excludeGroupedRecords[key];
    let rsCounter = value.filter(record => record.entity.productType === "RS").length;
    let attemptCounter = value.filter(record => record.entity.productType != "RS" && record.entity.attempt && parseInt(record.entity.attempt, 10) === 1).length;
    let counter = rsCounter + attemptCounter;
    return { [key]: counter };
  }).filter(entry => {
    const key = Object.keys(entry)[0];
    return entry[key] > 0;
  });
}

function getDeliveryWeek() {
  const dayOfWeek = parseInt(process.env.DELIVERY_DATE_DAY_OF_WEEK, 10) || 1;
  return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
}

async function updateExcludeCounter(excludeGroupedRecords, batchItemFailures) {
   
    const deliveryDate = getDeliveryWeek();
    let ttl = calculateTtl();
    const counterMap = retrieveCounterMap(excludeGroupedRecords);

    for (const mapEntry of counterMap) {
      let sk = Object.keys(mapEntry)[0];
      const inc = mapEntry[sk];
      try {
        const input = {
            TableName: counterTableName,
            Key: {
              deliveryDate: deliveryDate,
              sk: `EXCLUDE~${sk}`
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
        const failedRecords = excludeGroupedRecords[sk];
        if (failedRecords) {
          const failedSeqNumbers = failedRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; });
          batchItemFailures.push(...failedSeqNumbers);
        }
      }
    }
    return batchItemFailures;
}

async function batchWriteEvalCounter(groupedRecords, batchItemFailures) {
     const deliveryDate = getDeliveryWeek();
    let ttl = calculateTtl();
    const params = {
        RequestItems: {
          [counterTableName]: Object.keys(groupedRecords).map(key => ({
            PutRequest: {
              Item: {
                  deliveryDate: deliveryDate,
                  sk: `EVAL~${key}`,
                  ttl: ttl
              }
            }
          }))
        }
    }

    try {
        const command = new BatchWriteCommand(params);
        const response = await docClient.send(command);
        console.log(`Batch write successful for ${groupedRecords.length} items.`);

        const writeRequests = response.UnprocessedItems[counterTableName];
        if (writeRequests) {
          const failedIDs = [];
          console.log(`Unprocessed items: ${writeRequests.length}`);
          for (const writeRequest of writeRequests) {
            const unprocessedEntity = writeRequest.PutRequest.Item;
            const failedRecords = groupedRecords[unprocessedEntity.sk.S.split('~')[1]];
            if (failedRecords) {
              failedIDs.push(...failedRecords.map(record => record.kinesisSeqNumber));
            }
          }
          batchItemFailures = batchItemFailures.concat(failedIDs.map((i) => {
            return { itemIdentifier: i };
          }));
          console.warn("batchItemFailures:" + JSON.stringify(batchItemFailures));
        }
      } catch (error) {
        console.error('Error in batch write:', error);
        batchItemFailures = batchItemFailures.concat(
          Object.values(groupedRecords)
            .flat()
            .map(record => ({ itemIdentifier: record.kinesisSeqNumber }))
        );
      }
      return batchItemFailures;
}

async function batchWriteIncomingRecords(paperDeliveryIncomingRecords, batchItemFailures) {
  const batch_size = process.env.BATCH_SIZE;
  console.log(`Batch size: ${batch_size}`);
  const tableName = process.env.PAPER_DELIVERY_INCOMING_TABLE_NAME;

  const params = {
        RequestItems: {
          [tableName]: paperDeliveryIncomingRecords.map(record => ({
            PutRequest: {
              Item: record.entity
            }
          }))
        }
  }

  try {
    const command = new BatchWriteCommand(params);
    const response = await docClient.send(command);
    console.log(`Batch write successful for ${paperDeliveryIncomingRecords.length} items.`);

    const writeRequests = response.UnprocessedItems[tableName];
    if (writeRequests) {
      const failedIDs = [];
      console.log(`Unprocessed items: ${writeRequests.length}`);
      for (const writeRequest of writeRequests) {
        const unprocessedEntity = writeRequest.PutRequest.Item;
        const failedRecord = paperDeliveryIncomingRecords.find(record => record.entity.requestId === unprocessedEntity.requestId.S);
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
    batchItemFailures = batchItemFailures.concat(paperDeliveryIncomingRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; }));
  }
  return batchItemFailures;
}

async function batchWriteKinesisSequenceNumberRecords(eventRecords) {
  const tableName = process.env.KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME;
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

async function batchGetKinesisSequenceNumberRecords(keys) {
  const tableName = process.env.KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME;
  const params = {
    RequestItems: {
      [tableName]: {
        Keys: keys.map(key => (
            {
              sequenceNumber: key
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
    return items.map(item => item.sequenceNumber);
  });
}

module.exports = { batchWriteIncomingRecords,
                   batchWriteEvalCounter,
                   updateExcludeCounter,
                   batchWriteKinesisSequenceNumberRecords,
                   batchGetKinesisSequenceNumberRecords };
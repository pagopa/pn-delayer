const { extractKinesisData } = require("./lib/kinesis");
const {
  batchWriteIncomingRecords,
  updateExcludeCounter,
  batchWriteKinesisEventRecords,
  batchGetKinesisEventRecords
} = require("./lib/dynamo");
const {
  enrichWithSk,
  buildPaperDeliveryIncomingRecord,
  buildPaperDeliveryKinesisEventRecord,
  groupRecordsByProductAndProvince
} = require("./lib/utils");

exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));

  const kinesisData = extractKinesisData(event);
  if (!kinesisData || kinesisData.length === 0) {
    console.log("No events to process");
    return { batchItemFailures: [] };
  }

  let batchItemFailures = [];
  // Build records and remove duplicates by entity.requestId
  let paperDeliveryIncomingRecords = [];
  const requestIds = new Set();

  for (const eventItem of kinesisData) {
    const record = {
      entity: { ...buildPaperDeliveryIncomingRecord(eventItem) },
      kinesisSeqNumber: eventItem.kinesisSeqNumber
    };
    if (!requestIds.has(record.entity.requestId)) {
      requestIds.add(record.entity.requestId);
      paperDeliveryIncomingRecords.push(record);
    }
  }
  enrichWithSk(paperDeliveryIncomingRecords);

  const alreadyEvaluatedEvents = await batchGetKinesisEventRecords(
    paperDeliveryIncomingRecords.map(record => record.entity.requestId)
  );
  if (alreadyEvaluatedEvents.length > 0) {
    console.log("Skipping already evaluated events");
    paperDeliveryIncomingRecords = paperDeliveryIncomingRecords.filter(
      record => !alreadyEvaluatedEvents.includes(record.entity.requestId)
    );
  }

  if (paperDeliveryIncomingRecords.length > 0) {
    try {
      const groupedProductTypeProvinceRecords = groupRecordsByProductAndProvince(paperDeliveryIncomingRecords);

      for (const operation of [
        { func: updateExcludeCounter, data: groupedProductTypeProvinceRecords },
        { func: batchWriteIncomingRecords, data: paperDeliveryIncomingRecords }
      ]) {
        batchItemFailures = await operation.func(operation.data, batchItemFailures);
        paperDeliveryIncomingRecords = filterFailedRecords(paperDeliveryIncomingRecords, batchItemFailures);
        if (paperDeliveryIncomingRecords.length === 0) break;
      }
    } catch (error) {
      console.error("Error processing event", error);
    }
  }

  if (paperDeliveryIncomingRecords.length > 0) {
    const requestIds = paperDeliveryIncomingRecords.map(record =>
      buildPaperDeliveryKinesisEventRecord(record.entity.requestId)
    );
    await batchWriteKinesisEventRecords(requestIds);
    console.log(`Processed ${paperDeliveryIncomingRecords.length} records successfully`);
  } else {
    console.log("No new records to write to Kinesis sequence number table");
  }

  return { batchItemFailures };
};

function filterFailedRecords(records, failures) {
  return records.filter(record =>
    !failures.some(failure => failure.itemIdentifier === record.kinesisSeqNumber)
  );
}
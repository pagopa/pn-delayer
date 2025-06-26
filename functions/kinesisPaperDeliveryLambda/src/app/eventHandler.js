const { extractKinesisData } = require("./lib/kinesis");
const {
  batchWriteIncomingRecords,
  updateExcludeCounter,
  batchWriteKinesisSequenceNumberRecords,
  batchGetKinesisSequenceNumberRecords
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
  let paperDeliveryIncomingRecords = kinesisData.map(event => ({
    entity: { ...buildPaperDeliveryIncomingRecord(event) },
    kinesisSeqNumber: event.kinesisSeqNumber
  }));
  enrichWithSk(paperDeliveryIncomingRecords);

  const alreadyEvaluatedEvents = await batchGetKinesisSequenceNumberRecords(
    paperDeliveryIncomingRecords.map(record => record.kinesisSeqNumber)
  );
  if (alreadyEvaluatedEvents.length > 0) {
    console.log("Skipping already evaluated events");
    paperDeliveryIncomingRecords = paperDeliveryIncomingRecords.filter(
      record => !alreadyEvaluatedEvents.includes(record.kinesisSeqNumber)
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
    const sequenceNumbers = paperDeliveryIncomingRecords.map(record =>
      buildPaperDeliveryKinesisEventRecord(record.kinesisSeqNumber)
    );
    await batchWriteKinesisSequenceNumberRecords(sequenceNumbers);
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
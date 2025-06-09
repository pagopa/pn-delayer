const { extractKinesisData } = require("./lib/kinesis");
const { batchWriteHighPriorityRecords, batchWriteKinesisSequenceNumberRecords, batchGetKinesisSequenceNumberRecords } = require("./lib/dynamo");
const { enrichWithCreatedAt, buildPaperDeliveryHighPriorityRecord, buildPaperDeliveryKinesisEventRecord } = require("./lib/utils");

exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));

  const kinesisData = extractKinesisData(event);
  console.log(`KinesisData.length: ${kinesisData.length}`);
  let batchItemFailures = [];
  let alreadyEvaluatedEvents = []

  if (!kinesisData || kinesisData.length === 0) {
    console.log("No events to process");
    return { batchItemFailures };
  }

  let paperDeliveryHighPriorityRecords = kinesisData.map(event => ({entity : {...buildPaperDeliveryHighPriorityRecord(event)}, kinesisSeqNumber: event.kinesisSeqNumber}))
  enrichWithCreatedAt(paperDeliveryHighPriorityRecords);

  alreadyEvaluatedEvents = await batchGetKinesisSequenceNumberRecords(paperDeliveryHighPriorityRecords.map(record => record.kinesisSeqNumber));
  console.log(`Already evaluated events: ${alreadyEvaluatedEvents}`);

  if (alreadyEvaluatedEvents && alreadyEvaluatedEvents.length > 0) {
    console.log("Skipping already evaluated events");
    paperDeliveryHighPriorityRecords = paperDeliveryHighPriorityRecords.filter(record => !alreadyEvaluatedEvents.includes(record.kinesisSeqNumber));
  }

  try {
    batchItemFailures = await batchWriteHighPriorityRecords(paperDeliveryHighPriorityRecords);
  } catch (error) {
    console.error("Error processing event", error);
  }

  if (batchItemFailures.length > 0) {
    console.log("Process finished with errors!");
    paperDeliveryHighPriorityRecords = paperDeliveryHighPriorityRecords.filter(record => !batchItemFailures.some(failure => failure.itemIdentifier === record.kinesisSeqNumber));
  }

  if(paperDeliveryHighPriorityRecords.length === 0) {
    console.log("No new records to write to Kinesis sequence number table");
    return { batchItemFailures };
  }

  const sequenceNumbers = paperDeliveryHighPriorityRecords.map(record => buildPaperDeliveryKinesisEventRecord(record.kinesisSeqNumber));
  await batchWriteKinesisSequenceNumberRecords(sequenceNumbers);
  console.log(`Processed ${paperDeliveryHighPriorityRecords.length} records successfully`);
  return { batchItemFailures };
};




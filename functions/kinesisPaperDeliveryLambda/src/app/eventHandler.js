const { extractKinesisData } = require("./lib/kinesis");
const { batchWriteHighPriorityRecords, batchWriteKinesisSequenceNumberRecords, batchGetKinesisSequenceNumberRecords } = require("./lib/dynamo");
const { enrichWithCreatedAt, buildPaperDeliveryHighPriorityRecord } = require("./lib/utils");

exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));

  const kinesisData = extractKinesisData(event);
  console.log(`KinesisData.length: ${kinesisData.length}`);
  let batchItemFailures = [];

  if (!kinesisData || kinesisData.length === 0) {
    console.log("No events to process");
    return { batchItemFailures };
  }

  const paperDeliveryHighPriorityRecords = kinesisData.map(event => ({entity : {...buildPaperDeliveryHighPriorityRecord(event)}, kinesisSeqNumber: event.kinesisSeqNumber}))
  enrichWithCreatedAt(paperDeliveryHighPriorityRecords);

  const alreadyEvaluatedEvents = await batchGetKinesisSequenceNumberRecords(paperDeliveryHighPriorityRecords.map(record => record.kinesisSeqNumber));
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

  await batchWriteKinesisSequenceNumberRecords(paperDeliveryHighPriorityRecords)
  return { batchItemFailures };
};




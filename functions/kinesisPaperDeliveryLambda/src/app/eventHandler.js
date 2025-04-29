const { extractKinesisData } = require("./lib/kinesis");
const { batchWriteHighPriorityRecords } = require("./lib/dynamo");
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

  try {
    batchItemFailures = await batchWriteHighPriorityRecords(paperDeliveryHighPriorityRecords);
  } catch (error) {
    console.error("Error processing event", error);
  }

  if (batchItemFailures.length > 0) {
    console.log("Process finished with errors!");
  }

  return { batchItemFailures };
};




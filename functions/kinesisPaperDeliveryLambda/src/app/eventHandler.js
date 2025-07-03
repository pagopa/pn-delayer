const { extractKinesisData } = require("./lib/kinesis");
const { batchWriteHighPriorityRecords, batchWriteKinesisEventRecords, batchGetKinesisEventRecords } = require("./lib/dynamo");
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

  let paperDeliveryHighPriorityRecords = [];
  let requestIdsSet = new Set();

  for (const eventItem of kinesisData) {
    const record = {
        entity: { ...buildPaperDeliveryHighPriorityRecord(eventItem) },
        kinesisSeqNumber: eventItem.kinesisSeqNumber
    };
  if (!requestIdsSet.has(record.entity.requestId)) {
    requestIdsSet.add(record.entity.requestId);
    paperDeliveryHighPriorityRecords.push(record);
  }
}

  enrichWithCreatedAt(paperDeliveryHighPriorityRecords);

  alreadyEvaluatedEvents = await batchGetKinesisEventRecords(
      paperDeliveryHighPriorityRecords.map(record => record.entity.requestId)
    );

  if (alreadyEvaluatedEvents && alreadyEvaluatedEvents.length > 0) {
    console.log("Skipping already evaluated events");
    paperDeliveryHighPriorityRecords = paperDeliveryHighPriorityRecords.filter(record => !alreadyEvaluatedEvents.includes(record.entity.requestId));
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

  const kinesisEventRecords = paperDeliveryHighPriorityRecords.map(record =>buildPaperDeliveryKinesisEventRecord(record.entity.requestId));
  await batchWriteKinesisEventRecords(kinesisEventRecords);
  console.log(`Processed ${paperDeliveryHighPriorityRecords.length} records successfully`);
  return { batchItemFailures };
};




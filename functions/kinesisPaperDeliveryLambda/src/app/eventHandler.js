const { extractKinesisData } = require("./lib/kinesis");
const {
  batchWritePaperDeliveryRecords,
  updateExcludeCounter,
  batchWriteKinesisEventRecords,
  batchGetKinesisEventRecords
} = require("./lib/dynamo");
const {
  buildPaperDeliveryRecord,
  buildPaperDeliveryKinesisEventRecord,
  groupRecordsByProductAndProvince
} = require("./lib/utils");
const { LocalDate, DayOfWeek, TemporalAdjusters } = require("@js-joda/core");

exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));

  const kinesisData = extractKinesisData(event);
  if (!kinesisData || kinesisData.length === 0) {
    console.log("No events to process");
    return { batchItemFailures: [] };
  }

  const filteredData = filterInvalidRecords(kinesisData);

  if (filteredData.length === 0) {
    console.log("No valid event to process");
    return { batchItemFailures: [] };
  }

  let batchItemFailures = [];
  let paperDeliveryRecords = [];
  const requestIds = new Set();
  const dayOfWeek = parseInt(process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK, 10) || 1;
  const deliveryWeek = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(dayOfWeek))).toString();

  for (const eventItem of filteredData) {
    const record = {
      entity: { ...buildPaperDeliveryRecord(eventItem, deliveryWeek) },
      kinesisSeqNumber: eventItem.kinesisSeqNumber
    };
    if (!requestIds.has(record.entity.requestId)) {
      requestIds.add(record.entity.requestId);
      paperDeliveryRecords.push(record);
    }
  }

  const alreadyEvaluatedEvents = await batchGetKinesisEventRecords(
    paperDeliveryRecords.map(record => record.entity.requestId)
  );
  if (alreadyEvaluatedEvents.length > 0) {
    console.log("Skipping already evaluated events");
    paperDeliveryRecords = paperDeliveryRecords.filter(
      record => !alreadyEvaluatedEvents.includes(record.entity.requestId)
    );
  }

  if (paperDeliveryRecords.length > 0) {
    try {
      const groupedProductTypeProvinceRecords = groupRecordsByProductAndProvince(paperDeliveryRecords);

      for (const operation of [
        { func: updateExcludeCounter, data: groupedProductTypeProvinceRecords },
        { func: batchWritePaperDeliveryRecords, data: paperDeliveryRecords }
      ]) {
        batchItemFailures = await operation.func(operation.data, batchItemFailures);
        paperDeliveryRecords = filterFailedRecords(paperDeliveryRecords, batchItemFailures);
        if (paperDeliveryRecords.length === 0) break;
      }
    } catch (error) {
      console.error("Error processing event", error);
    }
  }

  if (paperDeliveryRecords.length > 0) {
    const requestIds = paperDeliveryRecords.map(record =>
      buildPaperDeliveryKinesisEventRecord(record.entity.requestId)
    );
    await batchWriteKinesisEventRecords(requestIds);
    console.log(`Processed ${paperDeliveryRecords.length} records successfully`);
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

function filterInvalidRecords(records) {
    const filteredData = [];
    for (const item of records) {
        if (item.attempt && item.prepareRequestDate && item.notificationSentAt) {
            filteredData.push(item);
        } else {
            console.warn(`Skipping invalid event: ${item.requestId}`);
        }
    }
    return filteredData;
}
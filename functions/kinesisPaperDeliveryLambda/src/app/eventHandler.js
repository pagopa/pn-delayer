const { extractKinesisData } = require("./lib/kinesis");
const {
  batchWritePaperDeliveryRecords,
  updateExcludeCounter,
  updateSenderPriorityCounter,
  batchWriteKinesisEventRecords,
  batchGetKinesisEventRecords,
  getSenderLimit,
  createDelayedCounter
} = require("./lib/dynamo");
const {
  buildPaperDeliveryRecord,
  buildPaperDeliveryKinesisEventRecord,
  groupRecordsByProductAndProvince,
  groupRecordsBySenderPaId,
  groupDelayedRecords,
  isCurrentWeek,
  getDeliveryWeek
} = require("./lib/utils");

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
  const delayedPaperDeliveryList = [];
  const requestIds = new Set();
  const deliveryWeek = getDeliveryWeek();

  // Separazione record per settimana corrente vs precedenti
  for (const eventItem of filteredData) {
    const inCurrentWeek = isCurrentWeek(eventItem.notificationSentAt);

    if (inCurrentWeek) {
      console.log("Current week");
      addPaperDeliveryRecordIfNew({
        eventItem,
        deliveryWeek,
        delayed: false,
        requestIds,
        paperDeliveryRecords
      });
    } else {
      console.log("Previous week");
      delayedPaperDeliveryList.push(eventItem);
    }
  }

  // Elaborazione record delayed
  if (delayedPaperDeliveryList.length > 0) {
    console.log(`Processing ${delayedPaperDeliveryList.length} delayed records`);
    const groupedDelayedRecords = groupDelayedRecords(delayedPaperDeliveryList);

    for (const [groupKey, groupRecords] of Object.entries(groupedDelayedRecords)) {
      const [notificationSentAtWeek, senderPaId, productType, province] = groupKey.split("~");

      console.log(`Processing delayed group: ${groupKey}`);

      const senderLimitItem = await getSenderLimit(
        senderPaId,
        productType,
        province,
        notificationSentAtWeek
      );

      let hasSenderLimit = senderLimitItem && senderLimitItem.weeklyEstimate > 0;

      if (hasSenderLimit) {
        console.log(`Found sender limit for ${groupKey}: ${senderLimitItem.weeklyEstimate}`);
        try {
          await createDelayedCounter(
            deliveryWeek,
            notificationSentAtWeek,
            senderPaId,
            productType,
            province,
            groupRecords.length,
            senderLimitItem.weeklyEstimate
          );
        } catch (error) {
          console.error(`Failed to create delayed counter for group ${groupKey}`, error);
          batchItemFailures.push(
            ...groupRecords.map(item => ({ itemIdentifier: item.kinesisSeqNumber }))
          );
          hasSenderLimit = false;
          continue;
        }
      } else {
        console.log(`No sender limit found for ${groupKey}`);
      }

      for (const eventItem of groupRecords) {
        addPaperDeliveryRecordIfNew({
          eventItem,
          deliveryWeek,
          delayed: !!hasSenderLimit,
          requestIds,
          paperDeliveryRecords
        });
      }
    }
  }

  // Verifica record già elaborati
  if (paperDeliveryRecords.length > 0) {
    const alreadyEvaluatedEvents = await batchGetKinesisEventRecords(
      paperDeliveryRecords.map(record => record.entity.requestId)
    );
    if (alreadyEvaluatedEvents.length > 0) {
      console.log("Skipping already evaluated events");
      paperDeliveryRecords = paperDeliveryRecords.filter(
        record => !alreadyEvaluatedEvents.includes(record.entity.requestId)
      );
    }
  }

  if (paperDeliveryRecords.length > 0) {
    try {
      const groupedProductTypeProvinceRecords = groupRecordsByProductAndProvince(paperDeliveryRecords);
      const groupedSenderPaIdRecords = groupRecordsBySenderPaId(paperDeliveryRecords);

      for (const operation of [
        { func: updateExcludeCounter, data: groupedProductTypeProvinceRecords },
        { func: updateSenderPriorityCounter, data: groupedSenderPaIdRecords },
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
    const kinesisEventRecords = paperDeliveryRecords.map(record =>
      buildPaperDeliveryKinesisEventRecord(record.entity.requestId)
    );
    await batchWriteKinesisEventRecords(kinesisEventRecords);
    console.log(`Processed ${paperDeliveryRecords.length} records successfully`);
  } else {
    console.log("No new records to write to Kinesis sequence number table");
  }

  return { batchItemFailures };
};

function addPaperDeliveryRecordIfNew({
  eventItem,
  deliveryWeek,
  delayed,
  requestIds,
  paperDeliveryRecords
}) {
  const record = {
    entity: { ...buildPaperDeliveryRecord(eventItem, deliveryWeek, delayed) },
    kinesisSeqNumber: eventItem.kinesisSeqNumber
  };

  if (!requestIds.has(record.entity.requestId)) {
    requestIds.add(record.entity.requestId);
    paperDeliveryRecords.push(record);
  }
}

function filterFailedRecords(records, failures) {
  return records.filter(
    record => !failures.some(failure => failure.itemIdentifier === record.kinesisSeqNumber)
  );
}

function filterInvalidRecords(records) {
  const filteredData = [];
  for (const item of records) {
    if (
      item.attempt !== undefined &&
      item.attempt !== null &&
      item.prepareRequestDate &&
      item.notificationSentAt
    ) {
      filteredData.push(item);
    } else {
      console.warn(`Skipping invalid event: ${item.requestId}`);
    }
  }
  return filteredData;
}

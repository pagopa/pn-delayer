const { extractKinesisData } = require("./lib/kinesis");
const {
  batchWritePaperDeliveryRecords,
  updateExcludeCounter,
  updateSenderPriorityCounter,
  batchWriteKinesisEventRecords,
  batchGetKinesisEventRecords,
  getSenderLimit,
  updateUsedSenderLimitAndInsertPaperDeliveries
} = require("./lib/dynamo");
const {
  buildPaperDeliveryKinesisEventRecord,
  groupRecordsByProductAndProvince,
  groupRecordsBySenderPaId,
  groupDelayedRecords,
  calculateNotificationSentAtWeek,
  getCurrentWeek,
  getDeliveryWeek,
  addPaperDeliveryRecord,
  isRsOrSecondAttempt
} = require("./lib/utils");

exports.handleEvent = async event => {
  console.log("Event received:", JSON.stringify(event));

  const kinesisData = extractKinesisData(event);

  if (!kinesisData?.length) {
    console.log("No events to process");
    return { batchItemFailures: [] };
  }

  let filteredData = filterInvalidRecords(kinesisData);

  if (!filteredData.length) {
    console.log("No valid events to process");
    return { batchItemFailures: [] };
  }

  try {
    const requestIdsToCheck = filteredData.map(
      record => record.requestId
    );

    const alreadyEvaluatedEvents = await batchGetKinesisEventRecords(requestIdsToCheck);

    if (alreadyEvaluatedEvents.length > 0) {
      const alreadyEvaluatedRequestIds = new Set(alreadyEvaluatedEvents);
      console.log(`Skipping ${alreadyEvaluatedEvents.length} already evaluated events`);
      filteredData = filteredData.filter(
        record =>
          !alreadyEvaluatedRequestIds.has(record.requestId)
      );
    }
  } catch (error) {
    console.error(
      "Failed to check already evaluated events",
      error
    );

    return {
      batchItemFailures: uniqueFailures(
        filteredData.map(record => ({
          itemIdentifier: record.kinesisSeqNumber
        }))
      )
    };
  }

  if (filteredData.length === 0) {
    console.log("All events were already processed");
    return { batchItemFailures: [] };
  }

  let batchItemFailures = [];
  let paperDeliveryRecords = [];
  const delayedPaperDeliveryList = [];
  const requestIds = new Set();
  const deliveryWeek = getDeliveryWeek();
  const currentWeek = getCurrentWeek();

  /*
   * Separazione dei record appartenenti alla settimana corrente
   * da quelli appartenenti alle settimane precedenti.
   */
  for (const eventItem of filteredData) {
    const notificationSentAtWeek = calculateNotificationSentAtWeek(eventItem.notificationSentAt);
    if(isRsOrSecondAttempt(eventItem)  && eventItem.communicationType !== 'INFORMAL') {
      addPaperDeliveryRecord({
        eventItem,
        deliveryWeek,
        delayed: false,
        skipSenderLimit: true,
        requestIds,
        paperDeliveryRecords
      });
      continue;
    }

    if (notificationSentAtWeek === currentWeek) {
      addPaperDeliveryRecord({
        eventItem,
        deliveryWeek,
        delayed: false,
        skipSenderLimit: false,
        requestIds,
        paperDeliveryRecords
      });
      continue;
    }

    console.log(`PaperDelivery ${eventItem.requestId} belongs to a previous notification week`);
    delayedPaperDeliveryList.push({
      ...eventItem,
      notificationSentAtWeek
    });
  }

  /*
   * Gli eventi delayed vengono raggruppati per:
   *
   * notificationSentAtWeek
   * + senderPaId
   * + productType
   * + province
   *
   * Per ogni gruppo viene cercato il relativo sender limit.
   */
  if (delayedPaperDeliveryList.length > 0) {
    console.log(
      `Processing ${delayedPaperDeliveryList.length} delayed records`
    );

    const groupedDelayedRecords =
      groupDelayedRecords(delayedPaperDeliveryList);

    for (
      const [groupKey, groupRecords]
      of Object.entries(groupedDelayedRecords)
    ) {
      console.log(`Processing delayed group: ${groupKey}`);

      const [
        notificationSentAtWeek,
        senderPaId,
        productType,
        province
      ] = groupKey.split("~");

      try {
        const senderLimitItem = await getSenderLimit(
          senderPaId,
          productType,
          province,
          notificationSentAtWeek
        );

        const hasSenderLimit =
          senderLimitItem?.weeklyEstimate > 0;

        if (hasSenderLimit) {
          console.log(
            `Found sender limit for ${groupKey}: ${senderLimitItem.weeklyEstimate}`
          );

          await updateUsedSenderLimitAndInsertPaperDeliveries(
            groupRecords,
            paperDeliveryRecords,
            notificationSentAtWeek,
            senderLimitItem.weeklyEstimate,
            batchItemFailures
          );

          console.log(
            `Updated used sender limit and inserted PaperDeliveries for delayed group ${groupKey}`
          );

          continue;
        }

        console.log(
          `No valid sender limit found for delayed group ${groupKey}`
        );

        addPaperDeliveryRecords({
          eventItems: groupRecords,
          deliveryWeek,
          delayed: true,
          skipSenderLimit: false,
          requestIds,
          paperDeliveryRecords
        });
      } catch (error) {
        console.error(
          `Failed to process delayed group ${groupKey}`,
          error
        );

        batchItemFailures.push(
          ...groupRecords.map(item => ({
            itemIdentifier: item.kinesisSeqNumber
          }))
        );
      }
    }
  }

  /*
   * Aggiornamento contatori e scrittura PaperDelivery.
   *
   * Tutte le spedizioni conformi, delayed e non delayed,
   * entrano nello stesso flusso:
   *
   * 1. aggiornamento contatori EXCLUDE;
   * 2. aggiornamento contatori SENDER_PRIORITY;
   * 3. scrittura dei PaperDeliveryRecord non già inseriti
   *    dalla transazione sul sender limit.
   *
   * Dopo ogni operazione vengono rimossi i record falliti.
   */
  const operations = [
    {
      func: updateExcludeCounter,
      getData: groupRecordsByProductAndProvince
    },
    {
      func: updateSenderPriorityCounter,
      getData: groupRecordsBySenderPaId
    },
    {
      func: batchWritePaperDeliveryRecords,
      getData: records =>
        records.filter(
           record => !record.entity.skipSenderLimit || isRsOrSecondAttempt(record.entity)
        )
    }
  ];

  for (const operation of operations) {
    if (paperDeliveryRecords.length === 0) {
      break;
    }

    const operationData =
      operation.getData(paperDeliveryRecords);

    if (isEmptyOperationData(operationData)) {
      continue;
    }

    try {
      batchItemFailures = await operation.func(
        operationData,
        batchItemFailures
      );
    } catch (error) {
      console.error(
        "Error processing DynamoDB operation",
        error
      );

      batchItemFailures.push(
        ...paperDeliveryRecords.map(record => ({
          itemIdentifier: record.kinesisSeqNumber
        }))
      );
    }

    paperDeliveryRecords = filterFailedRecords(
      paperDeliveryRecords,
      batchItemFailures
    );
  }

  /*
   * Vengono marcati come elaborati solamente i record
   * che hanno completato tutte le operazioni precedenti.
   */
  if (paperDeliveryRecords.length === 0) {
    console.log(
      "No new records to write to Kinesis sequence number table"
    );

    return {
      batchItemFailures:
        uniqueFailures(batchItemFailures)
    };
  }

  const kinesisEventRecords =
    paperDeliveryRecords
    .filter(record => !record.entity.skipSenderLimit || isRsOrSecondAttempt(record.entity))
    .map(record =>
      buildPaperDeliveryKinesisEventRecord(
        record.entity.requestId
      )
    );

  try {
    await batchWriteKinesisEventRecords(
      kinesisEventRecords
    );

    console.log(
      `Processed ${paperDeliveryRecords.length} records successfully`
    );
  } catch (error) {
    console.error(
      "Failed to write processed Kinesis event records",
      error
    );

    batchItemFailures.push(
      ...paperDeliveryRecords.map(record => ({
        itemIdentifier: record.kinesisSeqNumber
      }))
    );
  }

  return {
    batchItemFailures:
      uniqueFailures(batchItemFailures)
  };
};

function addPaperDeliveryRecords({
  eventItems,
  deliveryWeek,
  delayed,
  skipSenderLimit,
  requestIds,
  paperDeliveryRecords
}) {
  for (const eventItem of eventItems) {
    addPaperDeliveryRecord({
      eventItem,
      deliveryWeek,
      delayed,
      skipSenderLimit,
      requestIds,
      paperDeliveryRecords
    });
  }
}

function filterFailedRecords(records, failures) {
  const failedIdentifiers = new Set(
    failures.map(
      failure => failure.itemIdentifier
    )
  );

  return records.filter(
    record =>
      !failedIdentifiers.has(
        record.kinesisSeqNumber
      )
  );
}

function uniqueFailures(failures) {
  return [
    ...new Map(
      failures.map(failure => [
        failure.itemIdentifier,
        failure
      ])
    ).values()
  ];
}

function isEmptyOperationData(data) {
  return Array.isArray(data)
    ? data.length === 0
    : Object.keys(data).length === 0;
}

function filterInvalidRecords(records) {
  return records.filter(item => {
    const isValid =
      item.attempt !== undefined &&
      item.attempt !== null &&
      item.prepareRequestDate &&
      item.notificationSentAt;

    if (!isValid) {
      console.warn(
        `Skipping invalid event: ${item.requestId}`
      );
    }

    return isValid;
  });
}
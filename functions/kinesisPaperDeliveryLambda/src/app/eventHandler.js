const { extractKinesisData } = require("./lib/kinesis");
const {
  batchWritePaperDeliveryRecords,
  updateExcludeCounter,
  updateSenderPriorityCounter,
  batchWriteKinesisEventRecords,
  batchGetKinesisEventRecords,
  getSenderLimit,
  updateDelayedCounter
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
    console.log("No valid events to process");
    return { batchItemFailures: [] };
  }

  let batchItemFailures = [];
  let paperDeliveryRecords = [];
  const delayedPaperDeliveryList = [];
  const requestIds = new Set();
  const deliveryWeek = getDeliveryWeek();

  /*
   * Separazione dei record appartenenti alla settimana corrente
   * da quelli appartenenti alle settimane precedenti.
   */
  for (const eventItem of filteredData) {
    const inCurrentWeek = isCurrentWeek(eventItem.notificationSentAt);

    if (inCurrentWeek) {
      addPaperDeliveryRecordIfNew({
        eventItem,
        deliveryWeek,
        delayed: false,
        requestIds,
        paperDeliveryRecords
      });
    } else {
      console.log(`PaperDelivery ${eventItem.requestId} belongs to a previous notification week`);
      delayedPaperDeliveryList.push(eventItem);
    }
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
    console.log(`Processing ${delayedPaperDeliveryList.length} delayed records`);
    const groupedDelayedRecords = groupDelayedRecords(delayedPaperDeliveryList);

    for (const [groupKey, groupRecords] of Object.entries(groupedDelayedRecords)) {
      const [notificationSentAtWeek, senderPaId, productType, province] = groupKey.split("~");

      console.log(`Processing delayed group: ${groupKey}`);

    try {
      const senderLimitItem = await getSenderLimit(
        senderPaId,
        productType,
        province,
        notificationSentAtWeek
      );

      let hasSenderLimit = senderLimitItem && senderLimitItem.weeklyEstimate > 0;

     /*
     * Il contatore DELAYED viene incrementato solamente
     * quando esiste un sender limit con weeklyEstimate > 0.
     */
      if (hasSenderLimit) {
        console.log(`Found sender limit for ${groupKey}: ${senderLimitItem.weeklyEstimate}`);
          await updateDelayedCounter(
            deliveryWeek,
            notificationSentAtWeek,
            senderPaId,
            productType,
            province,
            groupRecords.length,
            senderLimitItem.weeklyEstimate
          );
          console.log(`Updated DELAYED counter for ${groupKey} with ${groupRecords.length} shipments`);
        } else {
          console.log(`No valid sender limit found for delayed group ${groupKey}`);
        }

     /*
     * Tutte le spedizioni conformi proseguono nel flusso comune.
     *
     * delayed è true solamente quando:
     * - la spedizione appartiene a una settimana precedente;
     * - esiste un weeklyEstimate valido.
     */
      for (const eventItem of groupRecords) {
        addPaperDeliveryRecordIfNew({
          eventItem,
          deliveryWeek,
          delayed: !!hasSenderLimit,
          requestIds,
          paperDeliveryRecords
        });
      }
    } catch (error) {
    console.error(error);
             console.error(
               `Failed to process delayed group ${groupKey}`,
               error
             );

             batchItemFailures.push(
               ...groupRecords.map(item => ({
                 itemIdentifier: item.kinesisSeqNumber
               }))
             );

             batchItemFailures = uniqueFailures(
               batchItemFailures
             );
           }
         }
  }

  // Verifica record già elaborati
  if (paperDeliveryRecords.length > 0) {
    try {
    const alreadyEvaluatedEvents = await batchGetKinesisEventRecords(
      paperDeliveryRecords.map(record => record.entity.requestId)
    );
    if (alreadyEvaluatedEvents.length > 0) {
      console.log("Skipping already evaluated events");
      paperDeliveryRecords = paperDeliveryRecords.filter(
        record => !alreadyEvaluatedEvents.includes(record.entity.requestId)
      );
    }
    } catch (error) {
      console.error(
        "Failed to check already evaluated events",
        error
      );

      batchItemFailures.push(
        ...paperDeliveryRecords.map(record => ({
          itemIdentifier: record.kinesisSeqNumber
        }))
      );

      batchItemFailures = uniqueFailures(
        batchItemFailures
      );

      paperDeliveryRecords = [];
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
   * 3. scrittura dei PaperDeliveryRecord.
   *
   * Dopo ogni operazione vengono rimossi i record falliti.
   * I dati dell'operazione successiva vengono calcolati
   * utilizzando solamente i record ancora validi.
   */
  if (paperDeliveryRecords.length > 0) {
    const operations = [
      {
        func: updateExcludeCounter,
        getData: records =>
          groupRecordsByProductAndProvince(records)
      },
      {
        func: updateSenderPriorityCounter,
        getData: records =>
          groupRecordsBySenderPaId(records)
      },
      {
        func: batchWritePaperDeliveryRecords,
        getData: records => records
      }
    ];

    for (const operation of operations) {
      try {
        batchItemFailures = await operation.func(
          operation.getData(paperDeliveryRecords),
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

      batchItemFailures = uniqueFailures(
        batchItemFailures
      );

      paperDeliveryRecords = filterFailedRecords(
        paperDeliveryRecords,
        batchItemFailures
      );

      if (paperDeliveryRecords.length === 0) {
        break;
      }
    }
  }

  /*
   * Vengono marcati come elaborati solamente i record
   * che hanno completato tutte le operazioni precedenti.
   */
  if (paperDeliveryRecords.length > 0) {
    const kinesisEventRecords = paperDeliveryRecords.map(record =>
      buildPaperDeliveryKinesisEventRecord(record.entity.requestId)
    );

    try {
    await batchWriteKinesisEventRecords(kinesisEventRecords);
    console.log(`Processed ${paperDeliveryRecords.length} records successfully`);

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
  } else {
    console.log("No new records to write to Kinesis sequence number table");
  }

  return {
    batchItemFailures: uniqueFailures(
      batchItemFailures
    )
  };
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

const { LocalDate, DayOfWeek, TemporalAdjusters } = require('@js-joda/core');
const { queryByPartitionKey, insertItemsBatch } = require('./dynamo');
const { buildPaperDeliveryRecord } = require('./utils');

const TABLE_NAME = process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;

exports.handleEvent = async (event = {}) => {
  const { executionLimit, lastEvaluatedKey } = event;

  if (!executionLimit || executionLimit <= 0) {
    throw new Error('executionLimit è obbligatorio e deve essere maggiore di 0');
  }

  const dayOfWeek = parseInt(process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK, 10) || 1;

  const deliveryDate = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(dayOfWeek)));
  const pk = `${deliveryDate}~EVALUATE_SENDER_LIMIT`;

  const result = await processQueryAndItems(
    pk,
    executionLimit,
    lastEvaluatedKey,
    deliveryDate,
    0,
    false
  );

  return {
    success: true,
    itemsProcessed: result.totalProcessedItems,
    lastEvaluatedKey: result.lastEvaluatedKey,
    completed: !result.lastEvaluatedKey
  };
};

async function processQueryAndItems(pk,executionLimit,lastEvaluatedKey,deliveryDate,totalProcessedItems,isWeek2) {
  const result = await queryByPartitionKey(pk, executionLimit - totalProcessedItems, lastEvaluatedKey);

  let processedCount = totalProcessedItems;

  if (result.items.length > 0) {
    const transformed = result.items.map(i => buildPaperDeliveryRecord(i, deliveryDate));
    await processItems(transformed, deliveryDate);
    processedCount += transformed.length;
  }

  // Condizione 1: lastEvaluatedKey assente e non abbiamo raggiunto il limite di esecuzione
  if (result.lastEvaluatedKey && processedCount < executionLimit) {
    return processQueryAndItems(
      pk,
      executionLimit,
      result.lastEvaluatedKey,
      deliveryDate,
      processedCount,
      isWeek2
    );
  }
  // Condizione 2: lastEvaluatedKey assente, non abbiamo raggiunto il limite di esecuzione e stiamo ancora processando la settimana corrente
  else if(!result.lastEvaluatedKey && processedCount < executionLimit && !isWeek2) {
     const nextDeliveryDate = deliveryDate.plusWeeks(1);
     const pkWeek2 = `${nextDeliveryDate}~EVALUATE_SENDER_LIMIT`;
     return processQueryAndItems(
       pkWeek2,
       executionLimit,
       null,
       nextDeliveryDate,
       processedCount,
       true
     );
   }

  // Condizione 3: LastEvaluatedKey presente ma abbiamo raggiunto il limite di esecuzione
  else if (result.lastEvaluatedKey && processedCount >= executionLimit) {
    return {
      totalProcessedItems: processedCount,
      lastEvaluatedKey: result.lastEvaluatedKey
    };
  }
  else{
    return {
      totalProcessedItems: processedCount,
      lastEvaluatedKey: null
    };
  }
}

async function processItems(items) {
    if (!Array.isArray(items) || items.length === 0) {
        return;
    }

    const putRequests = items.map(item => ({
        PutRequest: {
            Item: { ...item }
        }
    }));

    const unprocessed = await insertItemsBatch(putRequests, 0);

    if (unprocessed.length > 0) {
        console.error('Alcuni item non sono stati processati:', unprocessed);
        throw new Error(
            `Batch write failed: ${unprocessed.length} unprocessed items`
        );
    }
}
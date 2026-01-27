const { LocalDate, DayOfWeek, TemporalAdjusters } = require('@js-joda/core');
const { queryByPartitionKey, insertItemsBatch } = require('./lib/dynamo');
const { buildPaperDeliveryRecord } = require('./lib/utils');

exports.handleEvent = async (event = {}) => {
  const { executionLimit, lastEvaluatedKey, currentWeek } = event;

  if (!executionLimit || executionLimit <= 0) {
    throw new Error('executionLimit è obbligatorio e deve essere maggiore di 0');
  }

  const dayOfWeek = Number.parseInt(process.env.DELIVERYDATEDAYOFWEEK, 10) || 1;

  const deliveryDate = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
  let pk;
  if(currentWeek){
    pk = `${deliveryDate}~EVALUATE_SENDER_LIMIT`;
  }else{
    const nextDeliveryDate = deliveryDate.plusWeeks(1);
    pk = `${nextDeliveryDate}~EVALUATE_SENDER_LIMIT`;
  }

  const result = await processQueryAndItems(
    pk,
    executionLimit,
    lastEvaluatedKey,
    deliveryDate,
    0
  );

  return {
    success: true,
    itemsProcessed: result.totalProcessedItems,
    lastEvaluatedKey: result.lastEvaluatedKey,
    completed: !result.lastEvaluatedKey,
    delaySeconds: parseInt(process.env.DELAY_SECONDS, 10)
  };
};

async function processQueryAndItems(pk, executionLimit, lastEvaluatedKey, deliveryDate, totalProcessedItems) {
  const result = await queryByPartitionKey(pk, executionLimit - totalProcessedItems, lastEvaluatedKey);

  let processedCount = totalProcessedItems;

  if (result.items.length > 0) {
    const transformed = result.items.map(i => buildPaperDeliveryRecord(i, deliveryDate));
    await processItems(transformed);
    processedCount += transformed.length;
  }

  // Condizione 1: lastEvaluatedKey presente e non abbiamo raggiunto il limite di esecuzione
  if (result.lastEvaluatedKey && processedCount < executionLimit) {
    return processQueryAndItems(
      pk,
      executionLimit,
      result.lastEvaluatedKey,
      deliveryDate,
      processedCount
    );
  }
  // Condizione 2: LastEvaluatedKey presente ma abbiamo raggiunto il limite di esecuzione
  else if (result.lastEvaluatedKey && processedCount >= executionLimit) {
    return {
      totalProcessedItems: processedCount,
      lastEvaluatedKey: result.lastEvaluatedKey
    };
  }
  else {
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
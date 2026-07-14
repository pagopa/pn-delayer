const {
  queryLatestByRequestId,
  insertItemsBatch,
  moveItemsToDeleted
} = require('./lib/dynamo');

const { buildPaperDeliveryRecord } = require('./lib/utils');

exports.handleEvent = async (event = {}) => {
  const requestIds = event.requestIds;

  validateRequestIds(requestIds);

  const {
    itemsToInsert,
    itemsToMoveToDeleted,
    skippedRequestIds
  } = await buildItems(requestIds);

  await insertPaperDeliveryItems(itemsToInsert);
  await moveItemsToDeletedIfNeeded(itemsToMoveToDeleted);

  return skippedRequestIds;
};

function validateRequestIds(requestIds) {
  if (!Array.isArray(requestIds) || requestIds.length === 0) {
    throw new Error('requestIds è obbligatorio e deve essere una lista non vuota');
  }
}

async function buildItems(requestIds) {
  const itemsToInsert = [];
  const itemsToMoveToDeleted = [];
  const skippedRequestIds = [];

  for (const requestId of requestIds) {
    const item = await queryLatestByRequestId(requestId);

    if (!item) {
      console.warn(`Nessun item trovato per requestId=${requestId}`);
      skippedRequestIds.push(requestId);
      continue;
    }

    if (item.workflowStep !== 'EVALUATE_SENDER_LIMIT') {
      console.warn(
        `RequestId=${requestId} skippato perché workflowStep=${item.workflowStep}`
      );
      skippedRequestIds.push(requestId);
      continue;
    }

    itemsToMoveToDeleted.push(item);
    itemsToInsert.push(buildPaperDeliveryRecord(item));
  }

  return {
    itemsToInsert,
    itemsToMoveToDeleted,
    skippedRequestIds
  };
}

async function insertPaperDeliveryItems(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return;
  }

  const unprocessed = await insertItemsBatch(
    items.map(item => ({
      PutRequest: {
        Item: item
      }
    }))
  );

  if (unprocessed.length > 0) {
    console.error('Alcuni item non sono stati processati:', unprocessed);
    throw new Error(`Batch write failed: ${unprocessed.length} unprocessed items`);
  }
}

async function moveItemsToDeletedIfNeeded(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return;
  }

  await moveItemsToDeleted(items);
}
function buildPaperDeliveryRecord(payload) {
  const date = retrieveDate(payload);
  return {
    pk: buildPk(payload.deliveryDate),
    sk: buildSk(date, payload.requestId),
    requestId: payload.requestId,
    createdAt: new Date().toISOString(),
    notificationSentAt: payload.notificationSentAt,
    prepareRequestDate: payload.prepareRequestDate,
    productType: payload.productType,
    senderPaId: payload.senderPaId,
    province: payload.province,
    cap: payload.cap,
    attempt: payload.attempt,
    iun: payload.iun,
    unifiedDeliveryDriver: payload.unifiedDeliveryDriver,
    tenderId: payload.tenderId,
    priority: payload.priority,
    recipientId: payload.recipientId,
    deliveryDate: payload.deliveryDate,
    workflowStep: 'SENT_TO_PREPARE_PHASE_2',
    communicationType: payload.communicationType,
    senderPriority: payload.senderPriority,
    virtualNotificationSentAt: payload.virtualNotificationSentAt,
    oldSk: payload.oldSk
  };
}

function retrieveDate(payload) {
  const isFirstAttempt = payload.attempt && Number.parseInt(payload.attempt, 10) === 1;

  return payload.productType === 'RS' || isFirstAttempt
    ? payload.prepareRequestDate
    : payload.notificationSentAt;
}

function buildPk(deliveryDate) {
  return `${deliveryDate}~SENT_TO_PREPARE_PHASE_2`;
}

function buildSk(date, requestId) {
  return `${date}~${requestId}`;
}

function chunkArray(items, size) {
  if (!Array.isArray(items) || items.length === 0) {
    return [];
  }

  return Array.from(
    { length: Math.ceil(items.length / size) },
    (_, index) => items.slice(index * size, index * size + size)
  );
}

module.exports = {
  buildPaperDeliveryRecord,
  chunkArray
};
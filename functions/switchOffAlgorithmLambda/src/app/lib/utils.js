function buildPaperDeliveryRecord(payload, deliveryWeek) {
  let date = retrieveDate(payload);
  return {
    pk: buildPk(payload.deliveryDate),
    sk: buildSk(date, payload.requestId),
    requestId: payload.requestId,
    createdAt: new Date().toISOString(),
    notificationSentAt: payload.notificationSentAt,
    prepareRequestDate: payload.prepareRequestDate,
    priority: priority,
    productType: payload.productType,
    senderPaId: payload.senderPaId,
    province: payload.province,
    cap: payload.cap,
    attempt: payload.attempt,
    iun: payload.iun,
    unifiedDeliveryDriver: payload.unifiedDeliveryDriver,
    tenderId: payload.tenderId,
    recipientId: payload.recipientId,
    workflowStep: 'SENT_TO_PREPARE_PHASE_2',
    deliveryDate: payload.deliveryDate,
    communicationType: payload.communicationType,
    senderPriority: payload.senderPriority,
    virtualNotificationSentAt: payload.virtualNotificationSentAt,
    oldSk: payload.oldSk,
    delayed: payload.delayed,
    skipSenderLimit: payload.skipSenderLimit
  };
};

function retrieveDate(payload) {
    if (payload.productType === "RS" || (payload.attempt && parseInt(payload.attempt, 10) === 1)) {
      return payload.prepareRequestDate;
    } else {
      return payload.notificationSentAt;
    }
}

function buildPk(deliveryWeek) {
    return `${deliveryWeek}~SENT_TO_PREPARE_PHASE_2`;
}

function buildSk(date, requestId) {
    return `${date}~${requestId}`;
}

function chunkArray(messages, size) {
    return Array.from({ length: Math.ceil(messages.length / size) },
    (_, i) => messages.slice(i * size, i * size + size));
}

module.exports = { buildPaperDeliveryRecord, chunkArray };
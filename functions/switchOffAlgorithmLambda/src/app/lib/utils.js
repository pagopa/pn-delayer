function buildPaperDeliveryRecord(payload, deliveryWeek) {
  let date = retrieveDate(payload);
  let priority = calculatePriority(payload.productType, payload.attempt);
  return {
    pk: buildPk(deliveryWeek),
    sk: buildSk(priority, date, payload.requestId),
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

function buildSk(priority, date, requestId) {
    return `${priority}~${date}~${requestId}`;
}

function calculatePriority(productType, attempt) {
    const priorityMap = process.env.PAPER_DELIVERY_PRIORITY_PARAMETER;
    if (!priorityMap) {
        throw new Error('PAPER_DELIVERY_PRIORITY_PARAMETER not found');
    }

    const priorityObj = JSON.parse(priorityMap);
    const targetKey = `PRODUCT_${productType}.ATTEMPT_${attempt}`;
    for (const [priority, values] of Object.entries(priorityObj)) {
        if (values.includes(targetKey)) {
            return Number(priority);
        }
    }
    throw new Error(`Priority not found for ${targetKey}`);
}

function chunkArray(messages, size) {
    return Array.from({ length: Math.ceil(messages.length / size) },
    (_, i) => messages.slice(i * size, i * size + size));
}

module.exports = { buildPaperDeliveryRecord, chunkArray };
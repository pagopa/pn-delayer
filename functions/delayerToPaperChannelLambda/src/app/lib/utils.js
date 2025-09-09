const { LocalDate } = require('@js-joda/core');

function chunkArray(messages, size) {
    return Array.from({ length: Math.ceil(messages.length / size) },
    (_, i) => messages.slice(i * size, i * size + size));
}

function mapToPaperDeliveryForGivenStep(item, deliveryWeek, step) {
   if(step === 'EVALUATE_SENDER_LIMIT'){
      let deliveryWeekLocalDate = LocalDate.parse(deliveryWeek);
      deliveryWeek = deliveryWeekLocalDate.plusDays(7).toString();
    }

    const paperDelivery = {
      pk: `${deliveryWeek}~${step}`,
      sk: buildSk(step, item),
      requestId: item.requestId,
      createdAt: new Date().toISOString(),
      notificationSentAt: item.notificationSentAt,
      prepareRequestDate: item.prepareRequestDate,
      productType: item.productType,
      senderPaId: item.senderPaId,
      province: item.province,
      cap: item.cap,
      attempt: item.attempt,
      iun: item.iun,
      unifiedDeliveryDriver: item.unifiedDeliveryDriver,
      tenderId: item.tenderId,
      recipientId: item.recipientId,
      priority: item.priority,
      workflowStep: `${step}`
  };

  return Object.fromEntries(
      Object.entries(paperDelivery).filter(([_, value]) => value !== null && value !== undefined)
  );
}

function buildSk(workflowStepEnum, paperDelivery) {
  const isRS = paperDelivery.productType?.toLowerCase() === "rs";
  const isFirstAttempt = paperDelivery.attempt === 1;
  const date = isRS || isFirstAttempt ? paperDelivery.prepareRequestDate : paperDelivery.notificationSentAt;

  switch (workflowStepEnum) {
    case "EVALUATE_SENDER_LIMIT":
      return [paperDelivery.province, date, paperDelivery.requestId].join("~");

    case "SENT_TO_PREPARE_PHASE_2":
      return [paperDelivery.priority, date, paperDelivery.requestId].join("~");

    default:
      throw new Error(`Unsupported workflow step: ${workflowStepEnum}`);
  }
}

module.exports = {
    chunkArray,
    mapToPaperDeliveryForGivenStep,
    buildSk
}

function enrichWithCreatedAt(paperDeliveryHighPriorityRecords) {
  let millis = 1;
  return paperDeliveryHighPriorityRecords.map(item => {
    const now = new Date();
    item.entity.createdAt = new Date(now.getTime() + millis).toISOString();
    millis++;
    return item;
  });
}

function buildPaperDeliveryHighPriorityRecord(payload) {
  return {
    unifiedDeliveryDriverGeokey: `${payload.unifiedDeliveryDriver}~${payload.recipientNormalizedAddress.pr}`,
    requestId: payload.requestId,
    productType: payload.productType,
    cap: payload.recipientNormalizedAddress.cap,
    province: payload.recipientNormalizedAddress.pr,
    senderPaId: payload.senderPaId,
    unifiedDeliveryDriver: payload.unifiedDeliveryDriver,
    tenderId: payload.tenderId,
    iun: payload.iun
  };
};

module.exports = { enrichWithCreatedAt, buildPaperDeliveryHighPriorityRecord };
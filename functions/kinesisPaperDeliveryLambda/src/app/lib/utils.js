function enrichWithSk(paperDeliveryIncomingRecords) {
  return paperDeliveryIncomingRecords.map(item => {
    if(item.entity.productType === "RS" || (item.entity.attempt && parseInt(item.entity.attempt, 10) === 1)) {
      item.entity.sk = `${item.entity.prepareRequestDate}~${item.entity.requestId}`;
      return item;
    }else{
      item.entity.sk = `${item.entity.notificationSentAt}~${item.entity.requestId}`;
      return item;
    }
  });
}

function buildPaperDeliveryIncomingRecord(payload) {
  return {
    province: payload.recipientNormalizedAddress.pr,
    unifiedDeliveryDriverProvince: `${payload.unifiedDeliveryDriver}~${payload.recipientNormalizedAddress.pr}`,
    createdAt: new Date().toISOString(),
    requestId: payload.requestId,
    productType: payload.productType,
    cap: payload.recipientNormalizedAddress.cap,
    province: payload.recipientNormalizedAddress.pr,
    region: payload.recipientNormalizedAddress.region,
    senderPaId: payload.senderPaId,
    unifiedDeliveryDriver: payload.unifiedDeliveryDriver,
    tenderId: payload.tenderId,
    iun: payload.iun,
    notificationSentAt: payload.notificationSentAt,
    prepareRequestDate: payload.prepareRequestDate,
    attempt: payload.attempt
  };
};

function buildPaperDeliveryKinesisEventRecord(sequenceNumber) {
    const ttl = Math.floor(Date.now() / 1000) + Number(process.env.KINESIS_PAPER_DELIVERY_TTL_SECONDS);
  return {
    sequenceNumber: sequenceNumber,
    ttl: ttl
  };
};

const groupRecordsByProductAndProvince = (records) => {
  return records.reduce((acc, record) => {
    const key = `${record.entity.productType}~${record.entity.province}`;
    if (!acc[key]) {    
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

module.exports = { enrichWithSk, buildPaperDeliveryIncomingRecord, buildPaperDeliveryKinesisEventRecord, groupRecordsByProductAndProvince };
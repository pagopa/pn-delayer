function buildPaperDeliveryRecord(payload, deliveryWeek) {
  let date = retrieveDate(payload);
  return {
    pk: buildPk(deliveryWeek),
    sk: buildSk(payload.recipientNormalizedAddress.pr, date, payload.requestId),
    requestId: payload.requestId,
    createdAt: new Date().toISOString(),
    notificationSentAt: payload.notificationSentAt,
    prepareRequestDate: payload.prepareRequestDate,
    productType: payload.productType,
    senderPaId: payload.senderPaId,
    province: payload.recipientNormalizedAddress.pr,
    cap: payload.recipientNormalizedAddress.cap,
    attempt: payload.attempt,
    iun: payload.iun,
    unifiedDeliveryDriver: payload.unifiedDeliveryDriver,
    tenderId: payload.tenderId,
    recipientId: payload.recipientId,
  };
};

function retrieveDate(payload) {
    if(payload.productType === "RS" || (payload.attempt && parseInt(payload.attempt, 10) === 1)) {
      return payload.prepareRequestDate;
    }else{
      return payload.notificationSentAt;
    }
}

function buildPk(deliveryWeek) {
    return `${deliveryWeek}~EVALUATE_SENDER_LIMIT`;
}

function buildSk(province, date, requestId) {
    return `${province}~${date}~${requestId}`;
}

function buildPaperDeliveryKinesisEventRecord(requestId) {
    const ttl = Math.floor(Date.now() / 1000) + Number(process.env.KINESIS_EVENTSRECORDTTLSECONDS);
  return {
    requestId: requestId,
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

module.exports = { buildPaperDeliveryRecord, buildPaperDeliveryKinesisEventRecord, groupRecordsByProductAndProvince };
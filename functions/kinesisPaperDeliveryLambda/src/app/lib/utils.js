const { LocalDate, DayOfWeek, TemporalAdjusters, Clock, Instant, ZoneOffset} = require("@js-joda/core");
const dayOfWeekEnv = process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK;

function buildPaperDeliveryRecord(payload, deliveryWeek, delayed = false) {
  const skipSenderLimit = isRsOrSecondAttempt(payload);
  const date = skipSenderLimit   ? payload.prepareRequestDate : payload.notificationSentAt;

  const record = {
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
    communicationType: payload.communicationType || 'LEGAL',
    workflowStep: 'EVALUATE_SENDER_LIMIT',
    senderPriority: payload.senderPriority ? payload.senderPriority : 0,
    deliveryDate: deliveryWeek,
    delayed: Boolean(delayed),
    skipSenderLimit
  };

  if (payload.senderPaId && !skipSenderLimit) {
    record.senderPaIdOriginalSentAt = `${payload.senderPaId}~${date}`;
  }

  return record;
}

function getDeliveryWeek() {
  const dayOfWeek = parseInt(dayOfWeekEnv, 10) || 1;
  return LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(dayOfWeek))).toString();
}

function calculateNotificationSentAtWeek(notificationSentAt) {
  const dayOfWeek = parseInt(dayOfWeekEnv, 10) || 1;
  return Instant.parse(notificationSentAt)
    .atOffset(ZoneOffset.UTC)
    .toLocalDate()
    .with(
      TemporalAdjusters.previousOrSame(
        DayOfWeek.of(dayOfWeek)
      )
    )
    .toString();
}

function getCurrentWeek() {
  const dayOfWeek = parseInt(dayOfWeekEnv, 10) || 1;
  return LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
}

function isRsOrSecondAttempt(payload) {
  return payload.productType === 'RS' || (payload.attempt && parseInt(payload.attempt, 10) === 1);
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
}

function groupRecordsByProductAndProvince(records) {
  return records.reduce((acc, record) => {
    const key = `${record.entity.province}~${record.entity.productType}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

function groupRecordsBySenderPaId(records) {
  return records.reduce((acc, record) => {
    const key = record.entity.senderPaId;
    if (!key) {
      return acc;
    }
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

function groupDelayedRecords(records) {
  return records.reduce((acc, record) => {
    const key = `${record.notificationSentAtWeek}~${record.senderPaId}~${record.productType}~${record.province}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

module.exports = {
  buildPaperDeliveryRecord,
  buildPaperDeliveryKinesisEventRecord,
  groupRecordsByProductAndProvince,
  groupRecordsBySenderPaId,
  groupDelayedRecords,
  calculateNotificationSentAtWeek,
  getCurrentWeek,
  getDeliveryWeek
};

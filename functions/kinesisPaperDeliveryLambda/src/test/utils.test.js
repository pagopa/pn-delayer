const {
  buildPaperDeliveryRecord,
  buildPaperDeliveryKinesisEventRecord,
  groupRecordsByProductAndProvince,
  groupRecordsBySenderPaId,
  groupDelayedRecords,
  calculateNotificationSentAtWeek,
  getCurrentWeek,
  getDeliveryWeek,
  addPaperDeliveryRecord
} = require('../app/lib/utils');
const { expect } = require("chai");
const sinon = require("sinon");

describe('buildPaperDeliveryHighPriorityRecord', () => {
  it('builds a record with all required fields from payload', () => {
    const payload = {
      recipientNormalizedAddress: { pr: 'province1', cap: '12345', region: 'region1' },
      requestId: 'req1',
      productType: 'type1',
      senderPaId: 'sender1',
      tenderId: 'tender1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2024-01-01T00:00:00Z',
      unifiedDeliveryDriver: 'driver1',
      attempt: 0,
      iun: 'iun1',
      communicationType: 'INFORMAL'
    };
    const result = buildPaperDeliveryRecord(payload, '2025-07-07');
    expect(result).to.include({
      pk: '2025-07-07~EVALUATE_SENDER_LIMIT',
      sk: 'province1~2025-01-01T00:00:00Z~req1',
      recipientId: payload.recipientId,
      province: 'province1',
      requestId: 'req1',
      productType: 'type1',
      cap: '12345',
      senderPaId: 'sender1',
      unifiedDeliveryDriver: 'driver1',
      tenderId: 'tender1',
      iun: 'iun1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2024-01-01T00:00:00Z',
      attempt: 0,
      communicationType: 'INFORMAL',
      senderPaIdOriginalSentAt: 'sender1~2025-01-01T00:00:00Z',
      deliveryDate: '2025-07-07',
      delayed: false,
      skipSenderLimit: false
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
  });

  it('builds a second attempt record using prepareRequestDate', () => {
    const payload = {
      recipientNormalizedAddress: { pr: 'province1', cap: '12345', region: 'region1' },
      requestId: 'req1',
      productType: 'type1',
      senderPaId: 'sender1',
      tenderId: 'tender1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2024-01-01T00:00:00Z',
      unifiedDeliveryDriver: 'driver1',
      attempt: 1,
      iun: 'iun1'
    };
    const result = buildPaperDeliveryRecord(payload, '2025-07-07');
    expect(result).to.include({
      pk: '2025-07-07~EVALUATE_SENDER_LIMIT',
      sk: 'province1~2024-01-01T00:00:00Z~req1',
      recipientId: payload.recipientId,
      province: 'province1',
      requestId: 'req1',
      productType: 'type1',
      cap: '12345',
      senderPaId: 'sender1',
      unifiedDeliveryDriver: 'driver1',
      tenderId: 'tender1',
      iun: 'iun1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2024-01-01T00:00:00Z',
      attempt: 1,
      communicationType: 'LEGAL',
      deliveryDate: '2025-07-07',
      delayed: false,
      skipSenderLimit: false
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
    expect(result).to.not.have.property('senderPaIdOriginalSentAt');
  });


  it('builds an RS record using prepareRequestDate', () => {
    const payload = {
      recipientNormalizedAddress: { pr: 'province1', cap: '12345', region: 'region1' },
      requestId: 'req1',
      productType: 'RS',
      senderPaId: 'sender1',
      tenderId: 'tender1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2024-01-01T00:00:00Z',
      unifiedDeliveryDriver: 'driver1',
      attempt: 0,
      iun: 'iun1',
      communicationType: 'LEGAL'
    };
    const result = buildPaperDeliveryRecord(payload, '2025-07-07');
    expect(result).to.include({
      pk: '2025-07-07~EVALUATE_SENDER_LIMIT',
      sk: 'province1~2024-01-01T00:00:00Z~req1',
      recipientId: payload.recipientId,
      province: 'province1',
      requestId: 'req1',
      productType: 'RS',
      cap: '12345',
      senderPaId: 'sender1',
      unifiedDeliveryDriver: 'driver1',
      tenderId: 'tender1',
      iun: 'iun1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2024-01-01T00:00:00Z',
      attempt: 0,
      communicationType: 'LEGAL',
      deliveryDate: '2025-07-07',
      delayed: false,
      skipSenderLimit: false
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
    expect(result).to.not.have.property('senderPaIdOriginalSentAt');
  });

  it('throws an error when payload is missing required fields', () => {
    const payload = { unifiedDeliveryDriver: 'driver1' };
    expect(() => buildPaperDeliveryRecord(payload)).throw();
  });

  it('sets delayed and skipSenderLimit when requested', () => {
    const payload = {
      recipientNormalizedAddress: { pr: 'RM', cap: '00100' },
      requestId: 'req-delayed',
      productType: 'AR',
      senderPaId: 'sender1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2025-01-02T00:00:00Z',
      attempt: 0
    };

    const result = buildPaperDeliveryRecord(
      payload,
      '2025-07-07',
      true,
      true
    );

    expect(result.delayed).to.equal(true);
    expect(result.skipSenderLimit).to.equal(true);
    expect(result.sk).to.equal('RM~2025-01-01T00:00:00Z~req-delayed');
    expect(result.senderPaIdOriginalSentAt).to.equal('sender1~2025-01-01T00:00:00Z');
  });

  it('converts delayed and skipSenderLimit values to booleans', () => {
    const payload = {
      recipientNormalizedAddress: { pr: 'RM', cap: '00100' },
      requestId: 'req-delayed',
      productType: 'AR',
      senderPaId: 'sender1',
      notificationSentAt: '2025-01-01T00:00:00Z',
      prepareRequestDate: '2025-01-02T00:00:00Z',
      attempt: 0
    };

    const result = buildPaperDeliveryRecord(
      payload,
      '2025-07-07',
      1,
      'true'
    );

    expect(result.delayed).to.equal(true);
    expect(result.skipSenderLimit).to.equal(true);
  });

  describe('buildPaperDeliveryKinesisEventRecord', () => {
    beforeEach(() => {
      process.env.KINESIS_EVENTSRECORDTTLSECONDS = '3600';
    });

    it('returns a record with the provided requestId', () => {
      const result = buildPaperDeliveryKinesisEventRecord('req-123');
      expect(result.requestId).to.equal('req-123');
    });

    it('returns a ttl set to current epoch plus the configured seconds', () => {
      const before = Math.floor(Date.now() / 1000) + 3600;
      const result = buildPaperDeliveryKinesisEventRecord('req-123');
      const after = Math.floor(Date.now() / 1000) + 3600;
      expect(result.ttl).to.be.within(before, after);
    });

    it('returns NaN ttl when KINESIS_EVENTSRECORDTTLSECONDS is not set', () => {
      delete process.env.KINESIS_EVENTSRECORDTTLSECONDS;
      const result = buildPaperDeliveryKinesisEventRecord('req-123');
      expect(result.ttl).to.be.NaN;
    });

    it('returns a record with undefined requestId when called without arguments', () => {
      const result = buildPaperDeliveryKinesisEventRecord();
      expect(result.requestId).to.be.undefined;
    });
  });

  describe('groupRecordsByProductAndProvince', () => {
    it('returns an empty object for an empty records array', () => {
      const result = groupRecordsByProductAndProvince([]);
      expect(result).to.deep.equal({});
    });

    it('groups a single record under the correct province~productType key', () => {
      const records = [{ entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq1' }];
      const result = groupRecordsByProductAndProvince(records);
      expect(result).to.have.key('MI~RS');
      expect(result['MI~RS']).to.deep.equal(records);
    });

    it('groups two records with the same province and productType under the same key', () => {
      const r1 = { entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq2' };
      const result = groupRecordsByProductAndProvince([r1, r2]);
      expect(result['MI~RS']).to.deep.equal([r1, r2]);
    });

    it('creates separate groups for records with the same productType but different provinces', () => {
      const r1 = { entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { province: 'RM', productType: 'RS' }, kinesisSeqNumber: 'seq2' };
      const result = groupRecordsByProductAndProvince([r1, r2]);
      expect(result['MI~RS']).to.deep.equal([r1]);
      expect(result['RM~RS']).to.deep.equal([r2]);
    });

    it('creates separate groups for records with the same province but different productTypes', () => {
      const r1 = { entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { province: 'MI', productType: '890' }, kinesisSeqNumber: 'seq2' };
      const result = groupRecordsByProductAndProvince([r1, r2]);
      expect(result['MI~RS']).to.deep.equal([r1]);
      expect(result['MI~890']).to.deep.equal([r2]);
    });

    it('builds multiple groups correctly when records span different province and productType combinations', () => {
      const r1 = { entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { province: 'MI', productType: 'RS' }, kinesisSeqNumber: 'seq2' };
      const r3 = { entity: { province: 'RM', productType: '890' }, kinesisSeqNumber: 'seq3' };
      const result = groupRecordsByProductAndProvince([r1, r2, r3]);
      expect(Object.keys(result)).to.have.lengthOf(2);
      expect(result['MI~RS']).to.deep.equal([r1, r2]);
      expect(result['RM~890']).to.deep.equal([r3]);
    });
  });

  describe('groupRecordsBySenderPaId', () => {
    it('returns an empty object for an empty records array', () => {
      const result = groupRecordsBySenderPaId([]);
      expect(result).to.deep.equal({});
    });

    it('groups a single record under the correct senderPaId key', () => {
      const records = [{ entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq1' }];
      const result = groupRecordsBySenderPaId(records);
      expect(result).to.have.key('sender1');
      expect(result['sender1']).to.deep.equal(records);
    });

    it('groups two records with the same senderPaId under the same key', () => {
      const r1 = { entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq2' };
      const result = groupRecordsBySenderPaId([r1, r2]);
      expect(result['sender1']).to.deep.equal([r1, r2]);
    });

    it('creates separate groups for records with different senderPaIds', () => {
      const r1 = { entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { senderPaId: 'sender2' }, kinesisSeqNumber: 'seq2' };
      const result = groupRecordsBySenderPaId([r1, r2]);
      expect(result['sender1']).to.deep.equal([r1]);
      expect(result['sender2']).to.deep.equal([r2]);
    });

    it('builds multiple groups correctly when records span different senderPaId combinations', () => {
      const r1 = { entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq1' };
      const r2 = { entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq2' };
      const r3 = { entity: { senderPaId: 'sender2' }, kinesisSeqNumber: 'seq3' };
      const result = groupRecordsBySenderPaId([r1, r2, r3]);
      expect(Object.keys(result)).to.have.lengthOf(2);
      expect(result['sender1']).to.deep.equal([r1, r2]);
      expect(result['sender2']).to.deep.equal([r3]);
    });

    it('ignores records without a senderPaId', () => {
        const r1 = { entity: { senderPaId: 'sender1' }, kinesisSeqNumber: 'seq1' };
        const r2 = { entity: { }, kinesisSeqNumber: 'seq2' };
        const result = groupRecordsBySenderPaId([r1, r2]);
        expect(Object.keys(result)).to.have.lengthOf(1);
        expect(result['sender1']).to.deep.equal([r1]);
    });
  });

  describe('groupDelayedRecords', () => {
    it('returns an empty object for an empty records array', () => {
      const result = groupDelayedRecords([]);
      expect(result).to.deep.equal({});
    });

    it('groups delayed records by week, sender, product and province', () => {
      const r1 = {
        notificationSentAtWeek: '2025-05-19',
        senderPaId: 'sender1',
        productType: 'AR',
        recipientNormalizedAddress: { pr: 'RM' }
      };
      const r2 = {
        notificationSentAtWeek: '2025-05-19',
        senderPaId: 'sender1',
        productType: 'AR',
        recipientNormalizedAddress: { pr: 'RM' }
      };

      const result = groupDelayedRecords([r1, r2]);

      expect(result).to.deep.equal({
        '2025-05-19~sender1~AR~RM': [r1, r2]
      });
    });

    it('creates separate delayed groups when one grouping value changes', () => {
      const r1 = {
        notificationSentAtWeek: '2025-05-19',
        senderPaId: 'sender1',
        productType: 'AR',
        recipientNormalizedAddress: { pr: 'RM' }
      };
      const r2 = {
        notificationSentAtWeek: '2025-05-19',
        senderPaId: 'sender1',
        productType: 'AR',
        recipientNormalizedAddress: { pr: 'MI' }
      };

      const result = groupDelayedRecords([r1, r2]);

      expect(result['2025-05-19~sender1~AR~RM']).to.deep.equal([r1]);
      expect(result['2025-05-19~sender1~AR~MI']).to.deep.equal([r2]);
    });
  });

  describe('week calculations', () => {
    let clock;

    beforeEach(() => {
      process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK = '1';
      clock = sinon.useFakeTimers(new Date('2026-04-08T12:00:00Z').getTime());
    });

    afterEach(() => {
      clock.restore();
    });

    it('calculates the notification week using previous or same configured day', () => {
      const result = calculateNotificationSentAtWeek('2026-04-08T12:00:00Z');
      expect(result).to.equal('2026-04-06');
    });

    it('keeps the same day when notification is on the configured day', () => {
      const result = calculateNotificationSentAtWeek('2026-04-06T12:00:00Z');
      expect(result).to.equal('2026-04-06');
    });

    it('returns the start of the current week', () => {
      const result = getCurrentWeek();
      expect(result).to.equal('2026-04-06');
    });

    it('returns the next configured delivery day', () => {
      const result = getDeliveryWeek();
      expect(result).to.equal('2026-04-13');
    });
  });

  describe('addPaperDeliveryRecord', () => {
    function buildEvent(overrides = {}) {
      return {
        recipientNormalizedAddress: { pr: 'RM', cap: '00100' },
        requestId: 'request1',
        productType: 'AR',
        senderPaId: 'sender1',
        notificationSentAt: '2025-01-01T00:00:00Z',
        prepareRequestDate: '2025-01-02T00:00:00Z',
        attempt: 0,
        kinesisSeqNumber: 'seq1',
        ...overrides
      };
    }

    it('adds a PaperDelivery record to the target list', () => {
      const requestIds = new Set();
      const paperDeliveryRecords = [];
      const eventItem = buildEvent();

      addPaperDeliveryRecord({
        eventItem,
        deliveryWeek: '2025-07-07',
        delayed: true,
        skipSenderLimit: false,
        requestIds,
        paperDeliveryRecords
      });

      expect(paperDeliveryRecords).to.have.lengthOf(1);
      expect(paperDeliveryRecords[0].kinesisSeqNumber).to.equal('seq1');
      expect(paperDeliveryRecords[0].entity.requestId).to.equal('request1');
      expect(paperDeliveryRecords[0].entity.delayed).to.equal(true);
      expect(paperDeliveryRecords[0].entity.skipSenderLimit).to.equal(false);
      expect(requestIds.has('request1')).to.equal(true);
    });

    it('does not add duplicate requestIds', () => {
      const requestIds = new Set();
      const paperDeliveryRecords = [];
      const firstEvent = buildEvent();
      const secondEvent = buildEvent({
        kinesisSeqNumber: 'seq2'
      });

      addPaperDeliveryRecord({
        eventItem: firstEvent,
        deliveryWeek: '2025-07-07',
        delayed: false,
        skipSenderLimit: false,
        requestIds,
        paperDeliveryRecords
      });

      addPaperDeliveryRecord({
        eventItem: secondEvent,
        deliveryWeek: '2025-07-07',
        delayed: false,
        skipSenderLimit: false,
        requestIds,
        paperDeliveryRecords
      });

      expect(paperDeliveryRecords).to.have.lengthOf(1);
      expect(paperDeliveryRecords[0].kinesisSeqNumber).to.equal('seq1');
    });

    it('adds records with different requestIds', () => {
      const requestIds = new Set();
      const paperDeliveryRecords = [];

      addPaperDeliveryRecord({
        eventItem: buildEvent(),
        deliveryWeek: '2025-07-07',
        delayed: false,
        skipSenderLimit: false,
        requestIds,
        paperDeliveryRecords
      });

      addPaperDeliveryRecord({
        eventItem: buildEvent({
          requestId: 'request2',
          kinesisSeqNumber: 'seq2'
        }),
        deliveryWeek: '2025-07-07',
        delayed: false,
        skipSenderLimit: false,
        requestIds,
        paperDeliveryRecords
      });

      expect(paperDeliveryRecords).to.have.lengthOf(2);
      expect(requestIds).to.deep.equal(new Set([
        'request1',
        'request2'
      ]));
    });
  });

});
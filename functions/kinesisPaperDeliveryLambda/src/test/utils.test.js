const { buildPaperDeliveryRecord } = require('../app/lib/utils');
const { expect } = require("chai");

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
      communicationType: 'INFORMAL'
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
  });

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
      iun: 'iun1'
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
      communicationType: 'LEGAL'
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
  });


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
      communicationType: 'LEGAL'
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
      communicationType: 'LEGAL'
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
  });


  it('throws an error when payload is missing required fields', () => {
    const payload = { unifiedDeliveryDriver: 'driver1' };
    expect(() => buildPaperDeliveryRecord(payload)).throw();
  });

  const { buildPaperDeliveryKinesisEventRecord, groupRecordsByProductAndProvince, groupRecordsBySenderPaId } = require('../app/lib/utils');

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
  });

});
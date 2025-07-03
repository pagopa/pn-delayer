const { enrichWithSk, buildPaperDeliveryIncomingRecord } = require('../app/lib/utils');
const { expect } = require("chai");

describe('enrichWithSk', () => {
  it('adds unique sk to each record', () => {
    const records = [
      {entity:{productType: "RS", attempt: 0, notificationSentAt: '2025-01-01T00:00:00Z', prepareRequestDate: '2024-01-01T00:00:00Z', requestId: 'request1'}}, 
      {entity:{productType: "RS", attempt: 1, notificationSentAt: '2025-01-01T00:00:00Z', prepareRequestDate: '2024-01-01T00:00:00Z', requestId: 'request2'}},  
      {entity:{productType: "AR", attempt: 1, notificationSentAt: '2025-01-01T00:00:00Z', prepareRequestDate: '2024-01-01T00:00:00Z', requestId: 'request3'}}, 
      {entity:{productType: "AR", attempt: 0, notificationSentAt: '2025-01-01T00:00:00Z', prepareRequestDate: '2024-01-01T00:00:00Z', requestId: 'request4'}}, 
    ];
    const result = enrichWithSk(records);
    expect(result[0].entity.sk).equal(records[0].entity.prepareRequestDate + "~" + records[0].entity.requestId);
    expect(result[1].entity.sk).equal(records[1].entity.prepareRequestDate + "~" + records[1].entity.requestId);
    expect(result[2].entity.sk).equal(records[2].entity.prepareRequestDate + "~" + records[2].entity.requestId);
    expect(result[3].entity.sk).equal(records[3].entity.notificationSentAt + "~" + records[3].entity.requestId);
  });

  it('returns an empty array when input is empty', () => {
    const result = enrichWithSk([]);
    expect(result).to.deep.equal([]);
  });
});

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
      iun: 'iun1'
    };
    const result = buildPaperDeliveryIncomingRecord(payload);
    expect(result).to.include({
      province: 'province1',
      unifiedDeliveryDriverProvince: 'driver1~province1',
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
      region: 'region1'
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
  });

  it('throws an error when payload is missing required fields', () => {
    const payload = { unifiedDeliveryDriver: 'driver1' };
    expect(() => buildPaperDeliveryIncomingRecord(payload)).throw();
  });
});
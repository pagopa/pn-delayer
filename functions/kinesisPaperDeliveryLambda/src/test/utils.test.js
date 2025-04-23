const { enrichWithCreatedAt, buildPaperDeliveryHighPriorityRecord } = require('../app/lib/utils');
const { expect } = require("chai");

describe('enrichWithCreatedAt', () => {
  it('adds unique createdAt timestamps to each record', () => {
    const records = [{}, {}, {}];
    const result = enrichWithCreatedAt(records);
    expect(result[0].createdAt).not.equal(result[1].createdAt);
    expect(result[1].createdAt).not.equal(result[2].createdAt);
  });

  it('returns an empty array when input is empty', () => {
    const result = enrichWithCreatedAt([]);
    expect(result).to.deep.equal([]);
  });
});

describe('buildPaperDeliveryHighPriorityRecord', () => {
  it('builds a record with all required fields from payload', () => {
    const payload = {
      unifiedDeliveryDriver: 'driver1',
      recipientNormalizedAddress: { pr: 'province1', cap: '12345' },
      requestId: 'req1',
      productType: 'type1',
      senderPaId: 'sender1',
      tenderId: 'tender1',
      iun: 'iun1'
    };
    const result = buildPaperDeliveryHighPriorityRecord(payload);
    expect(result).to.deep.equal({
      unifiedDeliveryDriverGeokey: 'driver1~province1',
      requestId: 'req1',
      productType: 'type1',
      cap: '12345',
      province: 'province1',
      senderPaId: 'sender1',
      unifiedDeliveryDriver: 'driver1',
      tenderId: 'tender1',
      iun: 'iun1'
    });
  });

  it('throws an error when payload is missing required fields', () => {
    const payload = { unifiedDeliveryDriver: 'driver1' };
    expect(() => buildPaperDeliveryHighPriorityRecord(payload)).throw();
  });
});
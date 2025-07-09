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
    });
    expect(result).to.have.property('createdAt');
    expect(new Date(result.createdAt).toString()).to.not.equal('Invalid Date');
  });

  it('throws an error when payload is missing required fields', () => {
    const payload = { unifiedDeliveryDriver: 'driver1' };
    expect(() => buildPaperDeliveryRecord(payload)).throw();
  });
});
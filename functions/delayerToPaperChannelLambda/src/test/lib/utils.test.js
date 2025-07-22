const { expect } = require('chai');

// Importa le funzioni da testare
const {
  chunkArray,
  mapToPaperDeliveryForGivenStep,
  buildSk
} = require('../../app/lib/utils'); // modifica il path se necessario

describe('chunkArray', () => {
  it('should divide array into chunks of specified size', () => {
    const input = [1, 2, 3, 4, 5];
    const result = chunkArray(input, 2);
    expect(result).to.deep.equal([[1, 2], [3, 4], [5]]);
  });

  it('should return an empty array when input is empty', () => {
    const result = chunkArray([], 3);
    expect(result).to.deep.equal([]);
  });

  it('should return one chunk if size >= array length', () => {
    const result = chunkArray([1, 2], 10);
    expect(result).to.deep.equal([[1, 2]]);
  });
});

describe('mapToPaperDeliveryForGivenStep', () => {
  it('should map item and payload correctly', () => {
    global.payload = {
      requestId: "requestId",
      createdAt: new Date().toISOString(),
      notificationSentAt: "2025-01-01T00:00:00Z",
      prepareRequestDate: "2025-02-01T00:00:00Z",
      productType: "AR",
      senderPaId: "paId1",
      province: "RM",
      cap: "00100",
      attempt: "0",
      iun: "iun",
      unifiedDeliveryDriver: "POSTE",
      tenderId: "TENDER1",
      recipientId: "RecipientId",
      priority: 3
    };

    const deliveryWeek = '2025-01-01';

    const result = mapToPaperDeliveryForGivenStep(global.payload, deliveryWeek, 'SENT_TO_PREPARE_PHASE_2');
    const result2 = mapToPaperDeliveryForGivenStep(global.payload, deliveryWeek, 'EVALUATE_SENDER_LIMIT');

    expect(result).to.include({
      pk: '2025-01-01~SENT_TO_PREPARE_PHASE_2',
      sk: '3~2025-01-01T00:00:00Z~requestId',
      requestId: 'requestId',
      notificationSentAt: "2025-01-01T00:00:00Z",
      prepareRequestDate: "2025-02-01T00:00:00Z",
      productType: "AR",
      senderPaId: "paId1",
      province: "RM",
      cap: "00100",
      attempt: "0",
      iun: "iun",
      unifiedDeliveryDriver: "POSTE",
      tenderId: "TENDER1",
      recipientId: "RecipientId",
      priority: 3
    });

    expect(result2).to.include({
      pk: '2025-01-08~EVALUATE_SENDER_LIMIT',
      sk: 'RM~2025-01-01T00:00:00Z~requestId',
      requestId: 'requestId',
      notificationSentAt: "2025-01-01T00:00:00Z",
      prepareRequestDate: "2025-02-01T00:00:00Z",
      productType: "AR",
      senderPaId: "paId1",
      province: "RM",
      cap: "00100",
      attempt: "0",
      iun: "iun",
      unifiedDeliveryDriver: "POSTE",
      tenderId: "TENDER1",
      recipientId: "RecipientId",
      priority: 3
    });
  });
});

describe('buildSortKey', () => {
  const paperDelivery = {
    productType: 'RS',
    attempt: 1,
    prepareRequestDate: '2023-01-02T10:00:00.000Z',
    notificationSentAt: '2023-01-01T10:00:00.000Z',
    province: 'RM',
    priority: 'HIGH',
    requestId: 'REQ123',
  };

  it('should build sort key for EVALUATE_SENDER_LIMIT using province and prepareRequestDate', () => {
    const result = buildSk('EVALUATE_SENDER_LIMIT', paperDelivery);
    expect(result).to.equal('RM~2023-01-02T10:00:00.000Z~REQ123');
  });

  it('should build sort key for SENT_TO_PREPARE_PHASE_2 using priority and prepareRequestDate', () => {
    const result = buildSk('SENT_TO_PREPARE_PHASE_2', paperDelivery);
    expect(result).to.equal('HIGH~2023-01-02T10:00:00.000Z~REQ123');
  });

  it('should throw error for unsupported step', () => {
    expect(() => buildSk('UNKNOWN_STEP', paperDelivery)).to.throw('Unsupported workflow step: UNKNOWN_STEP');
  });
});

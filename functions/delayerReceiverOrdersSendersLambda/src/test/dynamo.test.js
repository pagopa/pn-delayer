'use strict';

const assert = require('assert');

// Imposta le env come nel runtime reale (override se necessario)
process.env.LIMIT_TABLE = 'pn-PaperDeliverySenderLimit';
process.env.COUNTERS_TABLE = 'pn-PaperDeliveryCounters';
process.env.PROVINCE_TABLE = 'pn-PaperChannelProvince';

const { mockClient } = require('aws-sdk-client-mock');

// Client/Command (AWS SDK v3)
const { DynamoDBDocumentClient, BatchWriteCommand, UpdateCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

// Modulo da testare
const { persistWeeklyEstimates } = require('../../src/app/dynamo');

// Mocks
const ddbMock = mockClient(DynamoDBDocumentClient);

// Dataset base
const base = {
  paId: 'PA123',
  productType: 'AR',
  province: 'RM',
  monthlyEstimate: 400,
  originalEstimate: 420,
  lastUpdate: '2025-01-31T23:59:59Z',
};

function firstFeb() {
  return [
    { ...base, deliveryDate: '2025-02-10', weekType: 'FULL',          weeklyEstimate: 21 },
    { ...base, deliveryDate: '2025-01-27', weekType: 'PARTIAL_START', weeklyEstimate: 6  },
    { ...base, deliveryDate: '2025-02-24', weekType: 'PARTIAL_END',   weeklyEstimate: 3  },
  ];
}


function firstMar() {
  return [
    { ...base, deliveryDate: '2025-03-10', weekType: 'FULL',          weeklyEstimate: 28 },
    { ...base, deliveryDate: '2025-02-24', weekType: 'PARTIAL_START', weeklyEstimate: 8  },
    { ...base, deliveryDate: '2025-03-31', weekType: 'PARTIAL_END',   weeklyEstimate: 4  },
  ];
}

describe('persistWeeklyEstimates — mesi separati (senza Jest)', () => {
  beforeEach(() => {
    ddbMock.reset();

    // Default: tutte le send rispondono OK
    ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} });
    ddbMock.on(UpdateCommand).resolves({});
    ddbMock.on(QueryCommand).resolves({ Items: [] });
  });

  it('SOLO FEBBRAIO: 1 FULL, inizio e fine parziali con campi corretti', async () => {
    await persistWeeklyEstimates(firstFeb(), 'fileKey_FEB');

    // 1) FULL -> BatchWrite con 1 PutRequest
    const bwCalls = ddbMock.commandCalls(BatchWriteCommand);
    assert.strictEqual(bwCalls.length, 1, 'Deve esserci una sola batch write per i FULL');

    const reqItems = bwCalls[0].args[0].input.RequestItems;
    const limitTable = process.env.LIMIT_TABLE;
    const puts = reqItems[limitTable];
    assert.ok(Array.isArray(puts), 'RequestItems[LIMIT_TABLE] deve essere un array');
    assert.strictEqual(puts.length, 1, 'Numero FULL attesi: 1');

    const fullItem = puts[0].PutRequest.Item;
    assert.strictEqual(fullItem.deliveryDate, '2025-02-10');
    assert.strictEqual(fullItem.weeklyEstimate, 21);
    assert.strictEqual(fullItem.fileKey, 'fileKey_FEB');

    // 2) Parziali -> UpdateCommand con #portion
    const updCalls = ddbMock.commandCalls(UpdateCommand);
    // NB: qui ci sono anche gli update dei contatori; filtriamo quelli con #portion
    const partialUpdates = updCalls
      .map(c => c.args[0].input)
      .filter(u => typeof u.UpdateExpression === 'string' && u.UpdateExpression.includes('#portion'));

    assert.strictEqual(partialUpdates.length, 2, 'Devono esserci esattamente 2 update parziali');

    // Iniziale (PARTIAL_START): 2025-01-27, secondWeekWeeklyEstimate, portion=6
    const startUpd = partialUpdates.find(u => u.Key.deliveryDate === '2025-01-27');
    assert.ok(startUpd, 'Manca l\'update parziale iniziale');
    assert.strictEqual(startUpd.ExpressionAttributeNames['#portion'], 'secondWeekWeeklyEstimate');
    assert.strictEqual(startUpd.ExpressionAttributeValues[':portion'], 6);
    assert.strictEqual(startUpd.ExpressionAttributeValues[':fk'], 'fileKey_FEB');

    // Finale (PARTIAL_END): 2025-02-24, firstWeekWeeklyEstimate, portion=3
    const endUpd = partialUpdates.find(u => u.Key.deliveryDate === '2025-02-24');
    assert.ok(endUpd, 'Manca l\'update parziale finale');
    assert.strictEqual(endUpd.ExpressionAttributeNames['#portion'], 'firstWeekWeeklyEstimate');
    assert.strictEqual(endUpd.ExpressionAttributeValues[':portion'], 3);
    assert.strictEqual(endUpd.ExpressionAttributeValues[':fk'], 'fileKey_FEB');
  });

  it('SOLO MARZO: 1 FULL, inizio e fine parziali con campi corretti', async () => {
    await persistWeeklyEstimates(firstMar(), 'fileKey_MAR');

    // 1) FULL -> BatchWrite con 1 PutRequest
    const bwCalls = ddbMock.commandCalls(BatchWriteCommand);
    assert.strictEqual(bwCalls.length, 1, 'Deve esserci una sola batch write per i FULL');

    const reqItems = bwCalls[0].args[0].input.RequestItems;
    const limitTable = process.env.LIMIT_TABLE;
    const puts = reqItems[limitTable];
    assert.ok(Array.isArray(puts), 'RequestItems[LIMIT_TABLE] deve essere un array');
    assert.strictEqual(puts.length, 1, 'Numero FULL attesi: 1');

    const fullItem = puts[0].PutRequest.Item;
    assert.strictEqual(fullItem.deliveryDate, '2025-03-10');
    assert.strictEqual(fullItem.weeklyEstimate, 28);
    assert.strictEqual(fullItem.fileKey, 'fileKey_MAR');

    // 2) Parziali -> UpdateCommand con #portion
    const updCalls = ddbMock.commandCalls(UpdateCommand);
    const partialUpdates = updCalls
      .map(c => c.args[0].input)
      .filter(u => typeof u.UpdateExpression === 'string' && u.UpdateExpression.includes('#portion'));

    assert.strictEqual(partialUpdates.length, 2, 'Devono esserci esattamente 2 update parziali');

    // Iniziale (PARTIAL_START): 2025-02-24, secondWeekWeeklyEstimate, portion=8
    const startUpd = partialUpdates.find(u => u.Key.deliveryDate === '2025-02-24');
    assert.ok(startUpd, 'Manca l\'update parziale iniziale');
    assert.strictEqual(startUpd.ExpressionAttributeNames['#portion'], 'secondWeekWeeklyEstimate');
    assert.strictEqual(startUpd.ExpressionAttributeValues[':portion'], 8);
    assert.strictEqual(startUpd.ExpressionAttributeValues[':fk'], 'fileKey_MAR');

    // Finale (PARTIAL_END): 2025-03-31, firstWeekWeeklyEstimate, portion=4
    const endUpd = partialUpdates.find(u => u.Key.deliveryDate === '2025-03-31');
    assert.ok(endUpd, 'Manca l\'update parziale finale');
    assert.strictEqual(endUpd.ExpressionAttributeNames['#portion'], 'firstWeekWeeklyEstimate');
    assert.strictEqual(endUpd.ExpressionAttributeValues[':portion'], 4);
    assert.strictEqual(endUpd.ExpressionAttributeValues[':fk'], 'fileKey_MAR');
  });
});

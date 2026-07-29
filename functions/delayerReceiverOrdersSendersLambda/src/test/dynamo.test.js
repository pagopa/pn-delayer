'use strict';

const assert = require('assert');

// Imposta le env come nel runtime reale (override se necessario)
process.env.LIMIT_TABLE = 'pn-PaperDeliverySenderLimit';
process.env.COUNTERS_TABLE = 'pn-PaperDeliveryCounters';
process.env.PROVINCE_TABLE = 'pn-PaperChannelProvince';

const { mockClient } = require('aws-sdk-client-mock');

// Client/Command (AWS SDK v3)
const {
  DynamoDBDocumentClient,
  BatchWriteCommand,
  UpdateCommand,
  QueryCommand,
} = require('@aws-sdk/lib-dynamodb');

// ✅ RIPRISTINATO IMPORT FUNZIONI TESTATE
const {
  getProvinceDistribution,
  existsSenderLimitByFileKey,
  persistWeeklyEstimates,
} = require('../../src/app/dynamo');

// ─── Mock ──────────────────────────────────────────────────────────────────
const ddbMock = mockClient(DynamoDBDocumentClient);

const LIMIT_TABLE = process.env.LIMIT_TABLE;
const COUNTERS_TABLE = process.env.COUNTERS_TABLE;
const PROVINCE_TABLE = process.env.PROVINCE_TABLE;

// ─── Dataset helpers ───────────────────────────────────────────────────────
const base = {
  paId: 'PA123',
  productType: 'AR',
  province: 'RM',
  monthlyEstimate: 400,
  originalEstimate: 420,
  lastUpdate: '2025-01-31T23:59:59Z',
  archiveProcessedAt: '2025-01-31T10:00:00Z',
};

function firstFeb() {
  return [
    { ...base, deliveryDate: '2025-02-10', weekType: 'FULL', weeklyEstimate: 21 },
    { ...base, deliveryDate: '2025-01-27', weekType: 'PARTIAL_FIRST_WEEK', weeklyEstimate: 6 },
    { ...base, deliveryDate: '2025-02-24', weekType: 'PARTIAL_LAST_WEEK', weeklyEstimate: 3 },
  ];
}

function firstMar() {
  return [
    { ...base, deliveryDate: '2025-03-10', weekType: 'FULL', weeklyEstimate: 28 },
    { ...base, deliveryDate: '2025-02-24', weekType: 'PARTIAL_FIRST_WEEK', weeklyEstimate: 8 },
    { ...base, deliveryDate: '2025-03-31', weekType: 'PARTIAL_LAST_WEEK', weeklyEstimate: 4 },
  ];
}

/** Genera n settimane FULL consecutive */
function onlyFulls(n) {
  return Array.from({ length: n }, (_, i) => ({
    ...base,
    deliveryDate: `2025-04-${String(i + 1).padStart(2, '0')}`,
    weekType: 'FULL',
    weeklyEstimate: 10 + i,
  }));
}

// ─── Local helpers per filtrare le mock calls ──────────────────────────────
function getPartialUpdates() {
  return ddbMock
    .commandCalls(UpdateCommand)
    .map(c => c.args[0].input)
    .filter(
      u =>
        u.TableName === LIMIT_TABLE &&
        typeof u.UpdateExpression === 'string' &&
        u.UpdateExpression.includes('#portion'),
    );
}

function getCounterUpdates() {
  return ddbMock
    .commandCalls(UpdateCommand)
    .map(c => c.args[0].input)
    .filter(u => u.TableName === COUNTERS_TABLE);
}

// ══════════════════════════════════════════════════════════════════════════════
// getProvinceDistribution
// ══════════════════════════════════════════════════════════════════════════════
describe('getProvinceDistribution', () => {
  beforeEach(() => ddbMock.reset());

  it('interroga PROVINCE_TABLE con la regione e restituisce gli items', async () => {
    const items = [{ region: 'Lazio', province: 'RM', percentageDistribution: 0.8 }];
    ddbMock.on(QueryCommand).resolves({ Items: items });

    const result = await getProvinceDistribution('Lazio');

    assert.deepStrictEqual(result, items);

    const calls = ddbMock.commandCalls(QueryCommand);
    assert.strictEqual(calls.length, 1);
    const params = calls[0].args[0].input;
    assert.strictEqual(params.TableName, PROVINCE_TABLE);
    assert.strictEqual(params.ExpressionAttributeValues[':region'], 'Lazio');
  });

  it('restituisce array vuoto se DynamoDB non trova items', async () => {
    ddbMock.on(QueryCommand).resolves({});
    const result = await getProvinceDistribution('Sardegna');
    assert.deepStrictEqual(result, []);
  });

  it('propaga eccezioni di DynamoDB', async () => {
    ddbMock.on(QueryCommand).rejects(new Error('ResourceNotFoundException'));
    await assert.rejects(
      () => getProvinceDistribution('Lazio'),
      /ResourceNotFoundException/,
    );
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// existsSenderLimitByFileKey
// ══════════════════════════════════════════════════════════════════════════════
describe('existsSenderLimitByFileKey', () => {
  beforeEach(() => ddbMock.reset());

  it('interroga LIMIT_TABLE tramite il GSI fileKey-index e restituisce Count', async () => {
    ddbMock.on(QueryCommand).resolves({ Count: 1 });

    const result = await existsSenderLimitByFileKey('myFileKey');

    assert.deepStrictEqual(result, { Count: 1 });

    const calls = ddbMock.commandCalls(QueryCommand);
    assert.strictEqual(calls.length, 1);
    const params = calls[0].args[0].input;
    assert.strictEqual(params.TableName, LIMIT_TABLE);
    assert.strictEqual(params.IndexName, 'fileKey-index');
    assert.strictEqual(params.ExpressionAttributeValues[':fk'], 'myFileKey');
    assert.strictEqual(params.Limit, 1);
    assert.strictEqual(params.Select, 'COUNT');
  });

  it('restituisce Count=0 quando non ci sono items con quella fileKey', async () => {
    ddbMock.on(QueryCommand).resolves({ Count: 0 });
    const result = await existsSenderLimitByFileKey('unknownKey');
    assert.deepStrictEqual(result, { Count: 0 });
  });

  it('propaga eccezioni di DynamoDB', async () => {
    ddbMock.on(QueryCommand).rejects(new Error('ServiceUnavailable'));
    await assert.rejects(
      () => existsSenderLimitByFileKey('fk'),
      /ServiceUnavailable/,
    );
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// persistWeeklyEstimates
// ══════════════════════════════════════════════════════════════════════════════
describe('persistWeeklyEstimates', () => {
  beforeEach(() => {
    ddbMock.reset();
    ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} });
    ddbMock.on(UpdateCommand).resolves({});
    ddbMock.on(QueryCommand).resolves({ Items: [] });
  });

  describe('array vuoto', () => {
    it('non chiama nessun comando DynamoDB', async () => {
      await persistWeeklyEstimates([], 'fk_empty');
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length, 0);
      assert.strictEqual(ddbMock.commandCalls(UpdateCommand).length, 0);
    });
  });

  describe('upsertPartial mapping', () => {
    it('PARTIAL_FIRST_WEEK: usa secondWeekWeeklyEstimate e mantiene fileKey/archiveFileKey con if_not_exists', async () => {
      const estimates = [
        {
          ...base,
          deliveryDate: '2025-01-27',
          weekType: 'PARTIAL_FIRST_WEEK',
          weeklyEstimate: 6,
        },
      ];

      await persistWeeklyEstimates(estimates, 'fk_current', 'afk_current');

      const partialUpdates = getPartialUpdates();
      assert.strictEqual(partialUpdates.length, 1);

      const upd = partialUpdates[0];

      // mapping porzioni: PARTIAL_FIRST_WEEK -> second
      assert.strictEqual(
        upd.ExpressionAttributeNames['#portion'],
        'secondWeekWeeklyEstimate',
      );
      assert.strictEqual(
        upd.ExpressionAttributeNames['#otherWeekPortion'],
        'firstWeekWeeklyEstimate',
      );

      // comportamento fileKey/archiveFileKey per prima settimana parziale
      assert.match(upd.UpdateExpression, /fileKey\s*=\s*if_not_exists\(fileKey,\s*:fk\)/);
      assert.match(
        upd.UpdateExpression,
        /archiveFileKey\s*=\s*if_not_exists\(archiveFileKey,\s*:afk\)/,
      );

      // monthly/original in if_not_exists
      assert.match(
        upd.UpdateExpression,
        /monthlyEstimate\s*=\s*if_not_exists\(monthlyEstimate,\s*:me\)/,
      );
      assert.match(
        upd.UpdateExpression,
        /originalEstimate\s*=\s*if_not_exists\(originalEstimate,\s*:oe\)/,
      );

      // verifica key e valori principali
      assert.strictEqual(upd.Key.pk, `${base.paId}~${base.productType}~${base.province}`);
      assert.strictEqual(upd.Key.deliveryDate, '2025-01-27');
      assert.strictEqual(upd.ExpressionAttributeValues[':portion'], 6);
      assert.strictEqual(upd.ExpressionAttributeValues[':fk'], 'fk_current');
      assert.strictEqual(upd.ExpressionAttributeValues[':afk'], 'afk_current');

      // con solo partial non devono esserci BatchWrite
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length, 0);

      // i contatori devono comunque essere aggiornati
      const counterUpdates = getCounterUpdates();
      assert.ok(counterUpdates.length >= 1);
    });

    it('PARTIAL_LAST_WEEK: usa firstWeekWeeklyEstimate e sovrascrive fileKey/archiveFileKey', async () => {
      const estimates = [
        {
          ...base,
          deliveryDate: '2025-02-24',
          weekType: 'PARTIAL_LAST_WEEK',
          weeklyEstimate: 3,
        },
      ];

      await persistWeeklyEstimates(estimates, 'fk_current', 'afk_current');

      const partialUpdates = getPartialUpdates();
      assert.strictEqual(partialUpdates.length, 1);

      const upd = partialUpdates[0];

      // mapping porzioni: PARTIAL_LAST_WEEK -> first
      assert.strictEqual(
        upd.ExpressionAttributeNames['#portion'],
        'firstWeekWeeklyEstimate',
      );
      assert.strictEqual(
        upd.ExpressionAttributeNames['#otherWeekPortion'],
        'secondWeekWeeklyEstimate',
      );

      // comportamento fileKey/archiveFileKey per ultima settimana parziale
      assert.match(upd.UpdateExpression, /fileKey\s*=\s*:fk,/);
      assert.match(upd.UpdateExpression, /archiveFileKey\s*=\s*:afk,/);

      // monthly/original in overwrite
      assert.match(upd.UpdateExpression, /monthlyEstimate\s*=\s*:me,/);
      assert.match(upd.UpdateExpression, /originalEstimate\s*=\s*:oe,/);

      // verifica key e valori principali
      assert.strictEqual(upd.Key.pk, `${base.paId}~${base.productType}~${base.province}`);
      assert.strictEqual(upd.Key.deliveryDate, '2025-02-24');
      assert.strictEqual(upd.ExpressionAttributeValues[':portion'], 3);
      assert.strictEqual(upd.ExpressionAttributeValues[':fk'], 'fk_current');
      assert.strictEqual(upd.ExpressionAttributeValues[':afk'], 'afk_current');

      // con solo partial non devono esserci BatchWrite
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length, 0);

      // i contatori devono comunque essere aggiornati
      const counterUpdates = getCounterUpdates();
      assert.ok(counterUpdates.length >= 1);
    });
  });

});
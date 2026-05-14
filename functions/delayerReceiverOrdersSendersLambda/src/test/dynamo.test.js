'use strict';

const assert = require('assert');

// Imposta le env come nel runtime reale (override se necessario)
process.env.LIMIT_TABLE = 'pn-PaperDeliverySenderLimit';
process.env.COUNTERS_TABLE = 'pn-PaperDeliveryCounters';
process.env.PROVINCE_TABLE = 'pn-PaperChannelProvince';

const { mockClient } = require('aws-sdk-client-mock');

// Client/Command (AWS SDK v3)
const { DynamoDBDocumentClient, BatchWriteCommand, UpdateCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

// ─── Mock ──────────────────────────────────────────────────────────────────
const ddbMock = mockClient(DynamoDBDocumentClient);

const LIMIT_TABLE    = process.env.LIMIT_TABLE;
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

  // ── Array vuoto ────────────────────────────────────────────────────────────
  describe('array vuoto', () => {
    it('non chiama nessun comando DynamoDB', async () => {
      await persistWeeklyEstimates([], 'fk_empty');
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length, 0);
      assert.strictEqual(ddbMock.commandCalls(UpdateCommand).length, 0);
    });
  });

  // ── FULL ──────────────────────────────────────────────────────────────────
  describe('FULL weeks — BatchWrite', () => {
    it('scrive correttamente tutti i campi del PutRequest', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk1');

      const bwCalls = ddbMock.commandCalls(BatchWriteCommand);
      assert.strictEqual(bwCalls.length, 1, 'Deve esserci esattamente 1 BatchWrite');

      const puts = bwCalls[0].args[0].input.RequestItems[LIMIT_TABLE];
      assert.strictEqual(puts.length, 1);

      const item = puts[0].PutRequest.Item;
      assert.strictEqual(item.pk, 'PA123~AR~RM');
      assert.strictEqual(item.deliveryDate, '2025-02-10');
      assert.strictEqual(item.weeklyEstimate, 21);
      assert.strictEqual(item.monthlyEstimate, 400);
      assert.strictEqual(item.originalEstimate, 420);
      assert.strictEqual(item.paId, 'PA123');
      assert.strictEqual(item.productType, 'AR');
      assert.strictEqual(item.province, 'RM');
      assert.strictEqual(item.fileKey, 'fk1');
      assert.ok(typeof item.ttl === 'number', 'ttl deve essere un numero');
    });

    it('non chiama BatchWrite se non ci sono FULL', async () => {
      const onlyPartials = [
        { ...base, deliveryDate: '2025-01-27', weekType: 'PARTIAL_START', weeklyEstimate: 6 },
      ];
      await persistWeeklyEstimates(onlyPartials, 'fk_p');
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length, 0);
    });

    it('divide in batch da ≤25 items quando si superano 25 FULL', async () => {
      const estimates = onlyFulls(30);
      await persistWeeklyEstimates(estimates, 'fk_bulk');

      const bwCalls = ddbMock.commandCalls(BatchWriteCommand);
      assert.strictEqual(bwCalls.length, 2, 'Devono esserci 2 batch (25 + 5)');

      const firstBatch  = bwCalls[0].args[0].input.RequestItems[LIMIT_TABLE];
      const secondBatch = bwCalls[1].args[0].input.RequestItems[LIMIT_TABLE];
      assert.strictEqual(firstBatch.length, 25);
      assert.strictEqual(secondBatch.length, 5);
    });

    it('esattamente 25 FULL → 1 solo batch', async () => {
      await persistWeeklyEstimates(onlyFulls(25), 'fk_25');
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length, 1);
    });
  });

  // ── batchWriteWithRetry ────────────────────────────────────────────────────
  describe('batchWriteWithRetry', () => {
    it('riprova automaticamente in caso di UnprocessedItems', async () => {
      let callCount = 0;
      ddbMock.on(BatchWriteCommand).callsFake(() => {
        callCount++;
        if (callCount === 1) {
          return { UnprocessedItems: { [LIMIT_TABLE]: [{ PutRequest: { Item: {} } }] } };
        }
        return { UnprocessedItems: {} };
      });

      const estimates = [{ ...base, deliveryDate: '2025-02-10', weekType: 'FULL', weeklyEstimate: 10 }];
      await persistWeeklyEstimates(estimates, 'fk_retry');

      assert.strictEqual(callCount, 2, 'Deve fare 2 chiamate: 1 con UnprocessedItems poi 1 di retry');
    });

    it('lancia errore se si superano maxRetries (5) con UnprocessedItems persistenti', async () => {
      ddbMock.on(BatchWriteCommand).resolves({
        UnprocessedItems: { [LIMIT_TABLE]: [{ PutRequest: { Item: {} } }] },
      });

      const estimates = [{ ...base, deliveryDate: '2025-02-10', weekType: 'FULL', weeklyEstimate: 10 }];
      await assert.rejects(
        () => persistWeeklyEstimates(estimates, 'fk_fail'),
        /Exceeded maxRetries/,
      );
    });
  });

  // ── PARTIAL weeks ──────────────────────────────────────────────────────────
  describe('PARTIAL weeks — UpdateCommand su LIMIT_TABLE', () => {
    it('SOLO FEBBRAIO: 1 FULL, 1 PARTIAL_START, 1 PARTIAL_END con attributi corretti', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fileKey_FEB');

      const bwCalls = ddbMock.commandCalls(BatchWriteCommand);
      assert.strictEqual(bwCalls.length, 1);
      assert.strictEqual(bwCalls[0].args[0].input.RequestItems[LIMIT_TABLE].length, 1);

      const partials = getPartialUpdates();
      assert.strictEqual(partials.length, 2, 'Devono esserci 2 update parziali');

      const startUpd = partials.find(u => u.Key.deliveryDate === '2025-01-27');
      assert.ok(startUpd, 'Manca update PARTIAL_START');
      assert.strictEqual(startUpd.ExpressionAttributeNames['#portion'], 'secondWeekWeeklyEstimate');
      assert.strictEqual(startUpd.ExpressionAttributeValues[':portion'], 6);
      assert.strictEqual(startUpd.ExpressionAttributeValues[':fk'], 'fileKey_FEB');

      const endUpd = partials.find(u => u.Key.deliveryDate === '2025-02-24');
      assert.ok(endUpd, 'Manca update PARTIAL_END');
      assert.strictEqual(endUpd.ExpressionAttributeNames['#portion'], 'firstWeekWeeklyEstimate');
      assert.strictEqual(endUpd.ExpressionAttributeValues[':portion'], 3);
      assert.strictEqual(endUpd.ExpressionAttributeValues[':fk'], 'fileKey_FEB');
    });

    it('SOLO MARZO: 1 FULL, 1 PARTIAL_START, 1 PARTIAL_END con attributi corretti', async () => {
      await persistWeeklyEstimates(firstMar(), 'fileKey_MAR');

      const partials = getPartialUpdates();
      assert.strictEqual(partials.length, 2);

      const startUpd = partials.find(u => u.Key.deliveryDate === '2025-02-24');
      assert.ok(startUpd, 'Manca update PARTIAL_START');
      assert.strictEqual(startUpd.ExpressionAttributeNames['#portion'], 'secondWeekWeeklyEstimate');
      assert.strictEqual(startUpd.ExpressionAttributeValues[':portion'], 8);

      const endUpd = partials.find(u => u.Key.deliveryDate === '2025-03-31');
      assert.ok(endUpd, 'Manca update PARTIAL_END');
      assert.strictEqual(endUpd.ExpressionAttributeNames['#portion'], 'firstWeekWeeklyEstimate');
      assert.strictEqual(endUpd.ExpressionAttributeValues[':portion'], 4);
    });

    it('PARTIAL usa pk composito paId~productType~province', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');
      getPartialUpdates().forEach(u => {
        assert.strictEqual(u.Key.pk, 'PA123~AR~RM');
      });
    });

    it('PARTIAL_START: weeklyEstimate usa if_not_exists(firstWeekWeeklyEstimate, 0) + portion', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');
      const startUpd = getPartialUpdates().find(u => u.Key.deliveryDate === '2025-01-27');
      assert.ok(startUpd.UpdateExpression.includes('if_not_exists(#otherWeekPortion, :zero) + :portion'));
      assert.strictEqual(startUpd.ExpressionAttributeNames['#otherWeekPortion'], 'firstWeekWeeklyEstimate');
    });

    it('PARTIAL_END: weeklyEstimate usa if_not_exists(secondWeekWeeklyEstimate, 0) + portion', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');
      const endUpd = getPartialUpdates().find(u => u.Key.deliveryDate === '2025-02-24');
      assert.ok(endUpd.UpdateExpression.includes('if_not_exists(#otherWeekPortion, :zero) + :portion'));
      assert.strictEqual(endUpd.ExpressionAttributeNames['#otherWeekPortion'], 'secondWeekWeeklyEstimate');
    });

    it('PARTIAL_START usa if_not_exists(fileKey, :fk) — preserva fileKey esistente', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fileKey_FEB');
      const startUpd = getPartialUpdates().find(u => u.Key.deliveryDate === '2025-01-27');
      assert.ok(
        startUpd.UpdateExpression.includes('fileKey = if_not_exists(fileKey, :fk)'),
        `PARTIAL_START deve preservare fileKey. UpdateExpression: ${startUpd.UpdateExpression}`,
      );
      // Non deve contenere l'assegnazione diretta "fileKey = :fk,"
      assert.ok(
        !startUpd.UpdateExpression.includes('fileKey = :fk,'),
        `PARTIAL_START non deve fare overwrite diretto. UpdateExpression: ${startUpd.UpdateExpression}`,
      );
    });

    it('PARTIAL_END usa fileKey = :fk — sovrascrive sempre il fileKey', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fileKey_FEB');
      const endUpd = getPartialUpdates().find(u => u.Key.deliveryDate === '2025-02-24');
      assert.ok(
        endUpd.UpdateExpression.includes('fileKey = :fk,'),
        `PARTIAL_END deve sovrascrivere fileKey. UpdateExpression: ${endUpd.UpdateExpression}`,
      );
      assert.ok(
        !endUpd.UpdateExpression.includes('if_not_exists(fileKey'),
        `PARTIAL_END non deve usare if_not_exists. UpdateExpression: ${endUpd.UpdateExpression}`,
      );
    });

    it('PARTIAL imposta monthlyEstimate/originalEstimate/productType/province/paId con if_not_exists', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');
      getPartialUpdates().forEach(u => {
        ['monthlyEstimate', 'originalEstimate', 'productType', 'province', 'paId'].forEach(attr => {
          assert.ok(
            u.UpdateExpression.includes(`if_not_exists(${attr}`),
            `Manca if_not_exists per ${attr} in: ${u.UpdateExpression}`,
          );
        });
      });
    });

    it('PARTIAL imposta ttl con if_not_exists', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');
      getPartialUpdates().forEach(u => {
        assert.ok(u.UpdateExpression.includes('if_not_exists(#ttl, :ttl)'));
        assert.strictEqual(u.ExpressionAttributeNames['#ttl'], 'ttl');
        assert.ok(typeof u.ExpressionAttributeValues[':ttl'] === 'number');
      });
    });
  });

  // ── Counters — FULL ────────────────────────────────────────────────────────
  describe('Counters — FULL (upsertFullSumEstimateCounter)', () => {
    const singleFull = () => [
      { ...base, deliveryDate: '2025-02-10', weekType: 'FULL', weeklyEstimate: 21 },
    ];

    it('chiama 2 UpdateCommand su COUNTERS_TABLE per ogni settimana FULL', async () => {
      await persistWeeklyEstimates(singleFull(), 'fk');
      assert.strictEqual(getCounterUpdates().length, 2, 'Devono esserci step1 + step2');
    });

    it('step1: resetta numberOfShipments=0 con condizione su fullWeekArchiveProcessedAt', async () => {
      await persistWeeklyEstimates(singleFull(), 'fk');
      const step1 = getCounterUpdates().find(
        u => u.UpdateExpression && u.UpdateExpression.includes('numberOfShipments = :zero'),
      );
      assert.ok(step1, 'Manca step1 reset contatore FULL');
      assert.ok(step1.ConditionExpression.includes('fullWeekArchiveProcessedAt'));
      assert.strictEqual(step1.Key.pk, '2025-02-10');
      assert.strictEqual(step1.Key.sk, 'SUM_ESTIMATES~AR~RM');
    });

    it('step2: incrementa con ADD e condizione su fullWeekArchiveProcessedAt', async () => {
      await persistWeeklyEstimates(singleFull(), 'fk');
      const step2 = getCounterUpdates().find(
        u => u.UpdateExpression && u.UpdateExpression.trim().startsWith('ADD'),
      );
      assert.ok(step2, 'Manca step2 ADD contatore FULL');
      assert.ok(step2.ConditionExpression.includes('fullWeekArchiveProcessedAt'));
      assert.strictEqual(step2.ExpressionAttributeValues[':inc'], 21);
    });

    it('step1: ConditionalCheckFailedException viene ignorato (versione già aggiornata)', async () => {
      ddbMock.on(UpdateCommand).callsFake(input => {
        if (
          input.TableName === COUNTERS_TABLE &&
          input.UpdateExpression &&
          input.UpdateExpression.includes('numberOfShipments = :zero')
        ) {
          const err = new Error('ConditionalCheckFailedException');
          err.name = 'ConditionalCheckFailedException';
          throw err;
        }
        return {};
      });

      await assert.doesNotReject(() => persistWeeklyEstimates(singleFull(), 'fk'));
    });

    it('step2: ConditionalCheckFailedException viene ignorato (ZIP obsoleto)', async () => {
      ddbMock.on(UpdateCommand).callsFake(input => {
        if (
          input.TableName === COUNTERS_TABLE &&
          input.UpdateExpression &&
          input.UpdateExpression.trim().startsWith('ADD')
        ) {
          const err = new Error('ConditionalCheckFailedException');
          err.name = 'ConditionalCheckFailedException';
          throw err;
        }
        return {};
      });

      await assert.doesNotReject(() => persistWeeklyEstimates(singleFull(), 'fk'));
    });

    it('altri errori in step1 vengono propagati', async () => {
      let callNum = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        if (++callNum === 1) throw new Error('ProvisionedThroughputExceededException');
        return {};
      });

      await assert.rejects(
        () => persistWeeklyEstimates(singleFull(), 'fk'),
        /ProvisionedThroughputExceededException/,
      );
    });

    it('altri errori in step2 vengono propagati', async () => {
      let callNum = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        if (++callNum === 2) throw new Error('InternalServerError');
        return {};
      });

      await assert.rejects(
        () => persistWeeklyEstimates(singleFull(), 'fk'),
        /InternalServerError/,
      );
    });
  });

  // ── Counters — PARTIAL ─────────────────────────────────────────────────────
  describe('Counters — PARTIAL (upsertPartialSumEstimateCounter)', () => {
    it('PARTIAL_START: step1 usa secondWeekNumberOfShipments e secondWeekArchiveProcessedAt', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');

      const step1 = getCounterUpdates().find(
        u =>
          u.Key.pk === '2025-01-27' &&
          u.ExpressionAttributeNames &&
          u.ExpressionAttributeNames['#portion'] === 'secondWeekNumberOfShipments',
      );
      assert.ok(step1, 'Manca step1 counter PARTIAL_START');
      assert.strictEqual(step1.ExpressionAttributeNames['#portionArchive'], 'secondWeekArchiveProcessedAt');
      assert.ok(step1.ConditionExpression.includes('attribute_not_exists'));
      assert.strictEqual(step1.Key.sk, 'SUM_ESTIMATES~AR~RM');
    });

    it('PARTIAL_START: step2 ADD secondWeekNumberOfShipments + numberOfShipments con valore corretto', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');

      const step2 = getCounterUpdates().find(
        u =>
          u.Key.pk === '2025-01-27' &&
          u.UpdateExpression &&
          u.UpdateExpression.trim().startsWith('ADD') &&
          u.ExpressionAttributeNames &&
          u.ExpressionAttributeNames['#portion'] === 'secondWeekNumberOfShipments',
      );
      assert.ok(step2, 'Manca step2 ADD contatore PARTIAL_START');
      assert.strictEqual(step2.ExpressionAttributeValues[':inc'], 6);
    });

    it('PARTIAL_END: step1 usa firstWeekNumberOfShipments e firstWeekArchiveProcessedAt', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');

      const step1 = getCounterUpdates().find(
        u =>
          u.Key.pk === '2025-02-24' &&
          u.ExpressionAttributeNames &&
          u.ExpressionAttributeNames['#portion'] === 'firstWeekNumberOfShipments',
      );
      assert.ok(step1, 'Manca step1 counter PARTIAL_END');
      assert.strictEqual(step1.ExpressionAttributeNames['#portionArchive'], 'firstWeekArchiveProcessedAt');
    });

    it('PARTIAL_END: step2 ADD firstWeekNumberOfShipments + numberOfShipments con valore corretto', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');

      const step2 = getCounterUpdates().find(
        u =>
          u.Key.pk === '2025-02-24' &&
          u.UpdateExpression &&
          u.UpdateExpression.trim().startsWith('ADD') &&
          u.ExpressionAttributeNames &&
          u.ExpressionAttributeNames['#portion'] === 'firstWeekNumberOfShipments',
      );
      assert.ok(step2, 'Manca step2 ADD contatore PARTIAL_END');
      assert.strictEqual(step2.ExpressionAttributeValues[':inc'], 3);
    });

    it('step1: ConditionalCheckFailedException viene ignorato per PARTIAL', async () => {
      ddbMock.on(UpdateCommand).callsFake(input => {
        if (
          input.TableName === COUNTERS_TABLE &&
          input.ConditionExpression &&
          input.ConditionExpression.includes('attribute_not_exists')
        ) {
          const err = new Error('ConditionalCheckFailedException');
          err.name = 'ConditionalCheckFailedException';
          throw err;
        }
        return {};
      });

      await assert.doesNotReject(() => persistWeeklyEstimates(firstFeb(), 'fk'));
    });

    it('step2: ConditionalCheckFailedException viene ignorato per PARTIAL (ZIP obsoleto)', async () => {
      ddbMock.on(UpdateCommand).callsFake(input => {
        if (
          input.TableName === COUNTERS_TABLE &&
          input.UpdateExpression &&
          input.UpdateExpression.trim().startsWith('ADD')
        ) {
          const err = new Error('ConditionalCheckFailedException');
          err.name = 'ConditionalCheckFailedException';
          throw err;
        }
        return {};
      });

      await assert.doesNotReject(() => persistWeeklyEstimates(firstFeb(), 'fk'));
    });

    it('step1 PARTIAL: altri errori vengono propagati', async () => {
      const estimates = [
        { ...base, deliveryDate: '2025-01-27', weekType: 'PARTIAL_START', weeklyEstimate: 6 },
      ];
      let callNum = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        if (++callNum === 1) throw new Error('ServiceUnavailable');
        return {};
      });

      await assert.rejects(
        () => persistWeeklyEstimates(estimates, 'fk'),
        /ServiceUnavailable/,
      );
    });

    it('PARTIAL: step1 inizializza numberOfShipments = if_not_exists(otherPortion, 0)', async () => {
      await persistWeeklyEstimates(firstFeb(), 'fk');

      const startStep1 = getCounterUpdates().find(
        u =>
          u.Key.pk === '2025-01-27' &&
          u.ExpressionAttributeNames &&
          u.ExpressionAttributeNames['#portion'] === 'secondWeekNumberOfShipments' &&
          u.UpdateExpression &&
          u.UpdateExpression.includes('#numberOfShipments = if_not_exists(#otherPortion, :zero)'),
      );
      assert.ok(startStep1, 'step1 PARTIAL_START deve inizializzare numberOfShipments con if_not_exists(otherPortion)');
      assert.strictEqual(
        startStep1.ExpressionAttributeNames['#otherPortion'],
        'firstWeekNumberOfShipments',
      );
    });
  });

  // ── Scenario misto febbraio + marzo (integrazione) ─────────────────────────
  describe('Scenario misto — febbraio + marzo insieme', () => {
    it('persiste correttamente 2 FULL e 4 PARTIAL quando si combinano i due mesi', async () => {
      const combined = [...firstFeb(), ...firstMar()];
      await persistWeeklyEstimates(combined, 'fk_combined');

      const bwCalls = ddbMock.commandCalls(BatchWriteCommand);
      assert.strictEqual(bwCalls.length, 1, 'Tutti i FULL entrano in un unico batch');
      const puts = bwCalls[0].args[0].input.RequestItems[LIMIT_TABLE];
      assert.strictEqual(puts.length, 2, 'Devono esserci 2 FULL');

      const partials = getPartialUpdates();
      assert.strictEqual(partials.length, 4, 'Devono esserci 4 update parziali (2 mesi × 2 tipi)');
    });
  });
});

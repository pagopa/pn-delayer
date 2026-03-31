const { expect } = require('chai');
const proxyquire = require('proxyquire');
const lambdaTester = require('lambda-tester');

describe('handleEvent Lambda', () => {
  let handler, safeStorageMock, algorithmMock, dynamoMock;

  beforeEach(() => {
    safeStorageMock = { downloadJson: () => Promise.resolve({}) };
    algorithmMock = { calculateWeeklyEstimates: () => Promise.resolve(['estimate']) };
    dynamoMock = {
      getProvinceDistribution: () => {},
      existsSenderLimitByFileKey: () => Promise.resolve({ Count: 0 })
    };

    handler = proxyquire('../app/eventHandler', {
      './safeStorage': safeStorageMock,
      './algorithm': algorithmMock,
      './dynamo': dynamoMock
    });
  });

  it('processa correttamente un record non duplicato', async () => {
    const event = {
      Records: [
        { messageId: 'msg1', body: JSON.stringify({ key: 'file1', tags: {"archiveProcessedAt":["2026-01-01T00:00:00Z"]}}) }
      ]
    };

    await lambdaTester(handler.handleEvent)
      .event(event)
      .expectResult(result => {
        expect(result.batchItemFailures).to.deep.equal([]);
      });
  });

  it('salta i record duplicati', async () => {
    dynamoMock.existsSenderLimitByFileKey = () => Promise.resolve({ Count: 1 });
    handler = proxyquire('../app/eventHandler', {
      './safeStorage': safeStorageMock,
      './algorithm': algorithmMock,
      './dynamo': dynamoMock
    });

    const event = {
      Records: [
        { messageId: 'msg2', body: JSON.stringify({ key: 'file2', tags: {"archiveProcessedAt":["2026-01-01T00:00:00Z"]}}) }
      ]
    };

    await lambdaTester(handler.handleEvent)
      .event(event)
      .expectResult(result => {
        expect(result.batchItemFailures).to.deep.equal([]);
      });
  });

  it('gestisce errori e aggiunge batchItemFailures', async () => {
    safeStorageMock.downloadJson = () => Promise.reject(new Error('download error'));
    handler = proxyquire('../app/eventHandler', {
      './safeStorage': safeStorageMock,
      './algorithm': algorithmMock,
      './dynamo': dynamoMock
    });

    const event = {
      Records: [
        { messageId: 'msg3', body: JSON.stringify({ key: 'file3', tags: {"archiveProcessedAt":["2026-01-01T00:00:00Z"]}}) }
      ]
    };

    await lambdaTester(handler.handleEvent)
      .event(event)
      .expectResult(result => {
        expect(result.batchItemFailures).to.deep.equal([{ itemIdentifier: 'msg3' }]);
      });
  });

  it('gestisce batch con più record e errori misti', async () => {
    let call = 0;
    dynamoMock.existsSenderLimitByFileKey = () => {
      call++;
      if (call === 1) return Promise.resolve({ Count: 0 });
      if (call === 2) return Promise.resolve({ Count: 1 });
      return Promise.resolve({ Count: 0 });
    };
    safeStorageMock.downloadJson = (key) => {
      if (key === 'fileC') return Promise.reject(new Error('fail'));
      return Promise.resolve({});
    };
    handler = proxyquire('../app/eventHandler', {
      './safeStorage': safeStorageMock,
      './algorithm': algorithmMock,
      './dynamo': dynamoMock
    });

    const event = {
      Records: [
        { messageId: 'msgA', body: JSON.stringify({ key: 'fileA', tags: {"archiveProcessedAt":["2026-01-01T00:00:00Z"]}}) },
        { messageId: 'msgB', body: JSON.stringify({ key: 'fileB', tags: {"archiveProcessedAt":["2026-01-01T00:00:00Z"]}}) },
        { messageId: 'msgC', body: JSON.stringify({ key: 'fileC', tags: {"archiveProcessedAt":["2026-01-01T00:00:00Z"]}}) }
      ]
    };

    await lambdaTester(handler.handleEvent)
      .event(event)
      .expectResult(result => {
        expect(result.batchItemFailures).to.deep.equal([{ itemIdentifier: 'msgC' }]);
      });
  });
});

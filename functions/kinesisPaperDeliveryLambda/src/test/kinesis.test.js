const { extractKinesisData } = require('../app/lib/kinesis');
const { expect } = require("chai");

describe('extractKinesisData', () => {
  it('returns an array of extracted body details from kinesisEvent', () => {
    const kinesisEvent = [
      { detail: { body: { key1: 'value1', key2: 'value2' } } },
      { detail: { body: { key3: 'value3', key4: 'value4' } } }
    ];
    const result = extractKinesisData(kinesisEvent);
    expect(result).to.deep.equal([
      { key1: 'value1', key2: 'value2' },
      { key3: 'value3', key4: 'value4' }
    ]);
  });

  it('returns an empty array when kinesisEvent is empty', () => {
    const kinesisEvent = [];
    const result = extractKinesisData(kinesisEvent);
    expect(result).to.deep.equal([]);
  });

  it('throws an error when kinesisEvent is null or undefined', () => {
    expect(() => extractKinesisData(null)).throw();
    expect(() => extractKinesisData(undefined)).throw();
  });

  it('handles missing detail or body gracefully by returning a singleton list', () => {
    const kinesisEvent = [
      { detail: {} },
      { detail: { body: null } },
      {},
      { detail: { body: { key3: 'value3', key4: 'value4' } } }
    ];
    const result = extractKinesisData(kinesisEvent);
    expect(result).to.deep.equal([
      { key3: 'value3', key4: 'value4' }
    ]);
  });
});
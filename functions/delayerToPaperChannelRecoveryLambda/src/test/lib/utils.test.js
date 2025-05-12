const { expect } = require("chai");
const { chunkArray } = require("../../app/lib/utils");

describe("utils.test.js", () => {

  it("splits array into chunks of given size", () => {
    const messages = [1, 2, 3, 4, 5];
    const size = 2;

    const expectedChunks = [
      [1, 2],
      [3, 4],
      [5],
    ];

    const result = chunkArray(messages, size);
    expect(result).to.deep.equal(expectedChunks);
  });

  it("returns an empty array when input is empty", () => {
    const messages = [];
    const size = 3;

    const expectedChunks = [];

    const result = chunkArray(messages, size);
    expect(result).to.deep.equal(expectedChunks);
  });

  it("handles chunk size larger than array length", () => {
    const messages = [1, 2];
    const size = 5;

    const expectedChunks = [[1, 2]];

    const result = chunkArray(messages, size);
    expect(result).to.deep.equal(expectedChunks);
  });

  it("handles chunk size of 1", () => {
    const messages = [1, 2, 3];
    const size = 1;

    const expectedChunks = [[1], [2], [3]];

    const result = chunkArray(messages, size);
    expect(result).to.deep.equal(expectedChunks);
  });
});
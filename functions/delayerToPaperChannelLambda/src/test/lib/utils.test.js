const { expect } = require("chai");
const { chunkArray } = require("../../app/lib/utils");
const { groupRecordsByProductAndProvince } = require("../../app/lib/utils");

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

  describe("groupRecordsByProductAndProvince", () => {
    it("groups items by productType and province", () => {
      const items = [
        { paperDeliveryIncoming: { productType: "A", province: "X" }, id: 1 },
        { paperDeliveryIncoming: { productType: "A", province: "X" }, id: 2 },
        { paperDeliveryIncoming: { productType: "B", province: "Y" }, id: 3 },
        { paperDeliveryIncoming: { productType: "A", province: "Y" }, id: 4 },
      ];

      const result = groupRecordsByProductAndProvince(items);

      expect(result).to.deep.equal({
        "A~X": [
          { paperDeliveryIncoming: { productType: "A", province: "X" }, id: 1 },
          { paperDeliveryIncoming: { productType: "A", province: "X" }, id: 2 },
        ],
        "B~Y": [
          { paperDeliveryIncoming: { productType: "B", province: "Y" }, id: 3 },
        ],
        "A~Y": [
          { paperDeliveryIncoming: { productType: "A", province: "Y" }, id: 4 },
        ],
      });
    });

    it("returns an empty object when input is empty", () => {
      const result = groupRecordsByProductAndProvince([]);
      expect(result).to.deep.equal({});
    });

    it("handles items with different productTypes and same province", () => {
      const items = [
        { paperDeliveryIncoming: { productType: "A", province: "Z" }, id: 1 },
        { paperDeliveryIncoming: { productType: "B", province: "Z" }, id: 2 },
      ];

      const result = groupRecordsByProductAndProvince(items);

      expect(result).to.deep.equal({
        "A~Z": [
          { paperDeliveryIncoming: { productType: "A", province: "Z" }, id: 1 },
        ],
        "B~Z": [
          { paperDeliveryIncoming: { productType: "B", province: "Z" }, id: 2 },
        ],
      });
    });

    it("handles items with missing paperDeliveryIncoming gracefully", () => {
      const items = [
        { paperDeliveryIncoming: { productType: "A", province: "X" }, id: 1 },
        { id: 2 }, // missing paperDeliveryIncoming
      ];

      // This will throw, so let's expect an error
      expect(() => groupRecordsByProductAndProvince(items)).to.throw();
    });
  });
});
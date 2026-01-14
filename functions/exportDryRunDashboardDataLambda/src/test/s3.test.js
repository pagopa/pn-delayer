const { expect } = require("chai");
const sinon = require("sinon");
const proxyquire = require("proxyquire").noCallThru();

class MockCopyObjectCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockDeleteObjectCommand {
  constructor(input) {
    this.input = input;
  }
}

function buildS3ClientMock(mockResponse) {
  return {
    lastCommand: null,
    async send(command) {
      this.lastCommand = command;
      return mockResponse;
    }
  };
}

describe("S3 Utils", () => {

  describe("copyS3Object", () => {
    it("invia correttamente un CopyObjectCommand e restituisce la risposta", async () => {

      const expectedResponse = { CopyObjectResult: "OK" };
      const mockClient = buildS3ClientMock(expectedResponse);

      const { copyS3Object } = proxyquire("../app/lib/s3", {
        "@aws-sdk/client-s3": {
          S3Client: function () {
            return mockClient;
          },
          CopyObjectCommand: MockCopyObjectCommand,
          DeleteObjectCommand: MockDeleteObjectCommand
        }
      });

      const bucket = "my-bucket";
      const oldKey = "old/file.txt";
      const newKey = "new/file.txt";

      const result = await copyS3Object(bucket, oldKey, newKey);

      expect(result).to.deep.equal(expectedResponse);
      expect(mockClient.lastCommand).to.be.instanceOf(MockCopyObjectCommand);

      // Verifica parametri del comando
      expect(mockClient.lastCommand.input).to.deep.equal({
        Bucket: bucket,
        CopySource: oldKey,
        Key: newKey
      });
    });
  });

  describe("deleteS3Object", () => {
    it("invia correttamente un DeleteObjectCommand e restituisce la risposta", async () => {

      const expectedResponse = { DeleteObjectResult: "OK" };
      const mockClient = buildS3ClientMock(expectedResponse);

      const { deleteS3Object } = proxyquire("../app/lib/s3", {
        "@aws-sdk/client-s3": {
          S3Client: function () {
            return mockClient;
          },
          CopyObjectCommand: MockCopyObjectCommand,
          DeleteObjectCommand: MockDeleteObjectCommand
        }
      });

      const bucket = "my-bucket";
      const key = "to-delete.txt";

      const result = await deleteS3Object(bucket, key);

      expect(result).to.deep.equal(expectedResponse);
      expect(mockClient.lastCommand).to.be.instanceOf(MockDeleteObjectCommand);

      // Verifica parametri del comando
      expect(mockClient.lastCommand.input).to.deep.equal({
        Bucket: bucket,
        Key: key
      });
    });
  });

});

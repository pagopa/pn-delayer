const assert = require("assert");
const proxyquire = require("proxyquire").noCallThru();

describe("s3.js", () => {
  let sendCallCount, sendArgs, sendReturnValue, getSignedUrlCallCount, getSignedUrlArgs, getSignedUrlReturnValue;
  let s3ClientMock, getSignedUrlMock;

  beforeEach(() => {
    sendCallCount = 0;
    sendArgs = [];
    sendReturnValue = undefined;
    getSignedUrlCallCount = 0;
    getSignedUrlArgs = [];
    getSignedUrlReturnValue = undefined;

    s3ClientMock = {
      send: async function(cmd) {
        sendCallCount++;
        sendArgs.push(cmd);
        return sendReturnValue;
      }
    };
    getSignedUrlMock = async function() {
      getSignedUrlCallCount++;
      getSignedUrlArgs.push(Array.from(arguments));
      return getSignedUrlReturnValue;
    };
  });

  function getS3Lib() {
    return proxyquire("../../app/lib/s3", {
      "@aws-sdk/client-s3": {
        S3Client: function () { return s3ClientMock; },
        DeleteObjectCommand: function (input) { this.input = input; },
        GetObjectCommand: function (input) { this.input = input; },
        PutObjectCommand: function (input) { this.input = input; }
      },
      "@aws-sdk/s3-request-presigner": {
        getSignedUrl: getSignedUrlMock
      }
    });
  }

  it("deleteS3Object chiama DeleteObjectCommand e restituisce la risposta", async () => {
    const { deleteS3Object } = getS3Lib();
    sendReturnValue = { DeleteObjectResult: "ok" };
    const res = await deleteS3Object("bucket", "key");
    assert.strictEqual(sendCallCount, 1);
    assert.deepStrictEqual(sendArgs[0].input, { Bucket: "bucket", Key: "key" });
    assert.deepStrictEqual(res, { DeleteObjectResult: "ok" });
  });

    it("getS3Object chiama GetObjectCommand e restituisce response.Body", async () => {
      const { getS3Object } = getS3Lib();

      const fakeBody = {
        pipe: () => {},
        on: () => {},
      };

      sendReturnValue = { Body: fakeBody };

      const res = await getS3Object("bucket", "key");

      assert.strictEqual(sendCallCount, 1);
      assert.deepStrictEqual(sendArgs[0].input, { Bucket: "bucket", Key: "key" });
      assert.strictEqual(res, fakeBody);
    });

  it("putS3Object chiama PutObjectCommand con i parametri corretti", async () => {
    const { putS3Object } = getS3Lib();
    sendReturnValue = undefined;
    await putS3Object("bucket", "key", "data", "text/csv");
    assert.strictEqual(sendCallCount, 1);
    assert.deepStrictEqual(sendArgs[0].input, { Bucket: "bucket", Key: "key", Body: "data", ContentType: "text/csv" });
  });

  it("generatePresignedDownloadUrl chiama getSignedUrl e restituisce la url", async () => {
    const { generatePresignedDownloadUrl } = getS3Lib();
    getSignedUrlReturnValue = "https://signed-url";
    const url = await generatePresignedDownloadUrl("bucket", "key", 123);
    assert.strictEqual(getSignedUrlCallCount, 1);
    assert.strictEqual(url, "https://signed-url");
  });
});

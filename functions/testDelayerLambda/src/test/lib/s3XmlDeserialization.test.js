const assert = require("assert");
const { Readable } = require("stream");
const { S3Client, CompleteMultipartUploadCommand } = require("@aws-sdk/client-s3");

// Regression: fast-xml-parser 5.7.0 broke @aws-sdk/xml-builder ("Invalid character '#' in entity name: \"#xD\""),
// failing every S3 XML response, e.g. the multipart calls issued by lib-storage Upload for files > 5 MB in GET_RESIDUAL_PAPERS.
describe("S3 XML response deserialization", () => {
  it("deserializes CompleteMultipartUpload response containing &#xD;", async () => {
    const xml =
      '<?xml version="1.0" encoding="UTF-8"?>\n' +
      '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
      "<Location>https://bucket.s3.amazonaws.com/residual-papers/file.csv</Location>" +
      "<Bucket>bucket</Bucket>" +
      "<Key>residual-papers/line&#xD;break.csv</Key>" +
      "<ETag>&quot;etag-2&quot;</ETag>" +
      "</CompleteMultipartUploadResult>";

    const client = new S3Client({
      region: "eu-south-1",
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
      requestHandler: {
        handle: async () => ({
          response: {
            statusCode: 200,
            headers: { "content-type": "application/xml" },
            body: Readable.from([Buffer.from(xml)]),
          },
        }),
      },
    });

    const result = await client.send(
      new CompleteMultipartUploadCommand({
        Bucket: "bucket",
        Key: "residual-papers/file.csv",
        UploadId: "upload-id",
        MultipartUpload: { Parts: [{ ETag: "etag-1", PartNumber: 1 }] },
      })
    );

    assert.strictEqual(result.Bucket, "bucket");
    assert.strictEqual(result.Key, "residual-papers/line\rbreak.csv");
    assert.strictEqual(result.ETag, '"etag-2"');
  });
});

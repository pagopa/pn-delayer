const {
  S3Client,
  GetObjectCommand,
  GetObjectCommandInput,
  DeleteObjectCommand
} = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { Upload } = require("@aws-sdk/lib-storage");
const csvParser = require("csv-parser");
const { stringify } = require("csv-stringify");
const { PassThrough } = require("stream");
const { pipeline } = require("stream/promises");

const s3Client = new S3Client();

async function deleteS3Object(bucketName, key) {
    console.log(`Deleting object ${key} from bucket ${bucketName}`);
    const input = { // DeleteObjectRequest
        Bucket: bucketName,
        Key: key, // required
    };
    const command = new DeleteObjectCommand(input);
    const response = await s3Client.send(command);
    return response;
}

function extractBucketAndKeyFromS3Path(s3Path) {
  if (!s3Path || !s3Path.startsWith("s3://")) {
    throw new Error(`Invalid S3 path: ${s3Path}`);
  }

  const withoutProtocol = s3Path.slice("s3://".length);
  const firstSlashIndex = withoutProtocol.indexOf("/");

  if (firstSlashIndex < 0) {
    throw new Error(`Invalid S3 path, missing key: ${s3Path}`);
  }

  return {
    bucket: withoutProtocol.slice(0, firstSlashIndex),
    key: withoutProtocol.slice(firstSlashIndex + 1),
  };
}

async function convertAthenaCsvToSemicolonCsv(targetBucket, athenaS3Path, targetKey) {
  console.log(`Converting Athena CSV to semicolon CSV`);
  console.log(`Source: ${athenaS3Path}`);
  console.log(`Target: s3://${targetBucket}/${targetKey}`);

  const { bucket: sourceBucket, key: sourceKey } =
    extractBucketAndKeyFromS3Path(athenaS3Path);

  const response = await s3Client.send(
    new GetObjectCommand({
      Bucket: sourceBucket,
      Key: sourceKey,
    })
  );

  if (!response.Body) {
    throw new Error(`Empty S3 object body for ${athenaS3Path}`);
  }

  const outputStream = new PassThrough();

  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: targetBucket,
      Key: targetKey,
      Body: outputStream,
      ContentType: "text/csv; charset=utf-8",
    },
  });

  const stringifyStream = stringify({
    header: true,
    delimiter: ";",
  });

  const uploadPromise = upload.done();

  await pipeline(
    response.Body,
    csvParser(),
    stringifyStream,
    outputStream
  );

  await uploadPromise;

  console.log(`CSV conversion completed: s3://${targetBucket}/${targetKey}`);
}

async function generatePresignedDownloadUrl(bucketName, key, expiresIn = 300) {
  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: key,
  });

  return getSignedUrl(s3Client, command, { expiresIn });
}

module.exports = {
  convertAthenaCsvToSemicolonCsv,
  generatePresignedDownloadUrl,
  deleteS3Object
};
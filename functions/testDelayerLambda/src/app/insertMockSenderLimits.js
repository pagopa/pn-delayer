const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

const s3Client = new S3Client({});
const lambdaClient = new LambdaClient({});

const EXPIRES_IN = 300;

exports.insertMockSenderLimits = async (params = []) => {
  const BUCKET_NAME = process.env.BUCKET_NAME;
  const EVENTFILEREADY_LAMBDA_ARN = process.env.EVENTFILEREADY_LAMBDA_ARN;

  const [zipFileName] = params;

  if (!zipFileName) {
    throw new Error("Required parameter must be [zipFileName]");
  }

  if (!BUCKET_NAME) {
    throw new Error("Environment variable BUCKET_NAME must be defined");
  }

  if (!EVENTFILEREADY_LAMBDA_ARN) {
    throw new Error("Environment variable EVENTFILEREADY_LAMBDA_ARN must be defined");
  }

  const downloadUrl = await generateDownloadUrl({
    fileName: zipFileName,
    bucketName: BUCKET_NAME,
  });

  await invokeFileReadyLambda(downloadUrl, EVENTFILEREADY_LAMBDA_ARN);

  console.log("Import senderLimit process started");

  return {
    message: "Import senderLimit process started"
  };
};

async function generateDownloadUrl({ fileName, bucketName }) {
  if (!fileName) {
    throw new Error("fileName is required");
  }

  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: fileName,
  });

  return getSignedUrl(s3Client, command, {
    expiresIn: EXPIRES_IN,
  });
}

async function invokeFileReadyLambda(downloadUrl, functionName) {
  const command = new InvokeCommand({
    FunctionName: functionName,
    InvocationType: "RequestResponse",
    Payload: JSON.stringify({
      httpMethod: "POST",
      resource: "/file-ready-event",
      body: JSON.stringify({
        mock: true,
        downloadUrl: downloadUrl,
        fileVersion: "1.0.0"
      })
    }),
  });

  await lambdaClient.send(command);
}
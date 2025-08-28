const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

const signUrlExpiration = process.env.SIGN_URL_EXPIRATION;;

const client = new S3Client();

async function putObject(bucket, fileName, fileBody) {
  const input = {
    Bucket: bucket,
    Key: fileName,
    Body: fileBody
  };

  const command = new PutObjectCommand(input);
  const response = await client.send(command);
  return response;
}

async function getpresigneUrlObject(bucket, fileName) {
  const input = {
    Bucket: bucket,
    Key: fileName
  }

  return await getSignedUrl(client, new GetObjectCommand(input), { expiresIn: Number(signUrlExpiration) })
}


module.exports = { putObject, getpresigneUrlObject }
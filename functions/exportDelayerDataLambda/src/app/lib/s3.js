const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { fromIni } = require("@aws-sdk/credential-provider-ini");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

const signUrlExpiration = process.env.SIGN_URL_EXPIRATION;;
//const signUrlExpiration = "300";

/*function awsClientCfg(profile) {
  const self = this;
  return {
    region: "eu-south-1",
    credentials: fromIni({
      profile: profile,
    })
  }
}*/

//const client = new S3Client(awsClientCfg('sso_pn-core-dev'));
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
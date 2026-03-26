const { S3Client, DeleteObjectCommand, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
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

async function getS3Object(bucketName, key) {
    console.log(`Reading object ${key} from bucket ${bucketName}`);
    const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
    const response = await s3Client.send(command);
    return response.Body;
}

async function putS3Object(bucketName, key, body, contentType = "text/csv") {
    console.log(`Uploading object ${key} to bucket ${bucketName}`);
    const command = new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ContentType: contentType });
    await s3Client.send(command);
}

async function generatePresignedDownloadUrl(bucketName, key, expiresIn = 300) {
    const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
    return await getSignedUrl(s3Client, command, { expiresIn });
}

module.exports = {
  deleteS3Object,
  getS3Object,
  putS3Object,
  generatePresignedDownloadUrl
};

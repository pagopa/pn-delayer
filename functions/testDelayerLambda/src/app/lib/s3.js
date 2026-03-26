const { S3Client, CopyObjectCommand, DeleteObjectCommand, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const s3Client = new S3Client();

async function copyS3Object(bucketName, oldKey, newKey) {
    console.log(`Copying object from ${oldKey} to ${newKey} in bucket ${bucketName}`);
    const input = { // CopyObjectRequest
        Bucket: bucketName,
        CopySource: oldKey, // required
        Key: newKey, // required
    };
    const command = new CopyObjectCommand(input);
    const response = await s3Client.send(command);
    return response;
}

async function deleteS3Object(bucketName, key) {
    const input = { // DeleteObjectRequest
        Bucket: bucketName,
        Key: key, // required
    };
    const command = new DeleteObjectCommand(input);
    const response = await s3Client.send(command);
    return response;
}

async function getS3Object(bucketName, key) {
    const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
    const response = await s3Client.send(command);
    return response.Body.transformToString("utf-8");
}

async function putS3Object(bucketName, key, body, contentType = "text/csv") {
    const command = new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ContentType: contentType });
    await s3Client.send(command);
}

async function generatePresignedDownloadUrl(bucketName, key, expiresIn = 300) {
    const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
    return await getSignedUrl(s3Client, command, { expiresIn });
}

module.exports = {
  deleteS3Object,
  copyS3Object,
  getS3Object,
  putS3Object,
  generatePresignedDownloadUrl
};

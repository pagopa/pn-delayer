const { S3Client, CopyObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");

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

module.exports = {
  deleteS3Object,
  copyS3Object
};

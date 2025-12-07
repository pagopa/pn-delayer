//create s3 client in order to upload files to s3
const { S3Client, CopyObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const { fromIni } = require("@aws-sdk/credential-provider-ini");

const s3Client = new S3Client();

//function that us copy sdk v3 to rename an object in s3
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

//delete s3 object using prefix and key
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

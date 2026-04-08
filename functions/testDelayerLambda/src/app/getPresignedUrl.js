"use strict";

const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const s3Presigner = require("@aws-sdk/s3-request-presigner");

const s3 = new S3Client({});

const PresignedUrlType = Object.freeze({
  UPLOAD: "UPLOAD",
  DOWNLOAD: "DOWNLOAD",
});

const EXPIRES_IN = 300;

/**
 * Genera una URL presignata per upload o download su S3
 * @param {Object} params
 * @param {string} params.fileName
 * @param {string} [params.checksumSha256B64]
 * @param {"UPLOAD"|"DOWNLOAD"} [params.presignedUrlType]
 * @returns {Promise<{uploadUrl?:string, downloadUrl?:string, key:string, requiredHeaders?:Record<string,string>, expiresIn:number}>}
 */

exports.getPresignedUrl = async (params = {}) => {
  const { fileName, checksumSha256B64, presignedUrlType = PresignedUrlType.UPLOAD } = params;

  const BUCKET_NAME = process.env.BUCKET_NAME;
  if (!BUCKET_NAME) {throw new Error("Environment variable BUCKET_NAME must be defined");}

  //validate enum value
  if (!Object.values(PresignedUrlType).includes(presignedUrlType)) {
    throw new Error(`presignedUrlType must be one of: ${Object.values(PresignedUrlType).join(", ")}`);
  }

  if (presignedUrlType === PresignedUrlType.DOWNLOAD) {
    return generateDownloadUrl({
      fileName,
      bucketName: BUCKET_NAME,
    });
  }

  return generateUploadUrl({
    fileName,
    checksumSha256B64,
    bucketName: BUCKET_NAME,
  });
}


//DOWNLOAD Flow
async function generateDownloadUrl({ fileName, bucketName }) {
  if (!fileName) {
    throw new Error("fileName is required");
  }

  const key = fileName;

  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: key,
  });

  const downloadUrl = await s3Presigner.getSignedUrl(s3, command, {
    expiresIn: EXPIRES_IN,
  });

  return {
    downloadUrl,
    key,
    expiresIn: EXPIRES_IN,
  };
}

//UPLOAD Flow
async function generateUploadUrl({ fileName, checksumSha256B64, bucketName }) {
  if (!fileName) {
    throw new Error("fileName is required");
  }

  if (!checksumSha256B64) {
    throw new Error("checksumSha256B64 is required");
  }

  if (!/\.csv$/i.test(fileName) && !/\.zip$/i.test(fileName)) {
    throw new Error("fileName must end with .csv or .zip");
  }

  let key = `${Date.now()}-${fileName}`;

  let contentType = "text/csv";
  if (/\.zip$/i.test(fileName)){
    key = fileName;
    contentType = "application/zip"
  }
  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    ContentType: contentType,
    ChecksumSHA256: checksumSha256B64,
  });

  const uploadUrl = await s3Presigner.getSignedUrl(s3, command, {
    expiresIn: EXPIRES_IN,
  });

  return {
    uploadUrl,
    key,
    requiredHeaders: {
      "Content-Type": contentType,
      "x-amz-checksum-sha256": checksumSha256B64,
    },
    expiresIn: EXPIRES_IN,
  };

};
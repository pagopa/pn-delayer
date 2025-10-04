"use strict";

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

const s3 = new S3Client({});

/**
 * Genera una URL presignata per caricare un CSV su S3 (PUT).
 * @param {Array<string>} params [fileName, checksumSha256B64]
 * @returns {Promise<{uploadUrl:string, key:string, requiredHeaders:Record<string,string>, expiresIn:number}>}
 */
exports.getPresignedUrl = async (params = []) => {
  const [fileName, checksumSha256B64] = params;
  const BUCKET_NAME = process.env.BUCKET_NAME;

  if (!BUCKET_NAME) {
    throw new Error("Environment variable BUCKET_NAME must be defined");
  }
  if (!fileName || !checksumSha256B64) {
    throw new Error("Required parameters are [fileName, checksumSha256B64]");
  }
  if (!/\.csv$/i.test(fileName)) {
    throw new Error("fileName must end with .csv");
  }

  const key = `${Date.now()}-${fileName}`;
  const command = new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    ContentType: "text/csv",
    ChecksumSHA256: checksumSha256B64,
  });

  const expiresIn = 300;
  const uploadUrl = await getSignedUrl(s3, command, { expiresIn });

  return {
    uploadUrl,
    key,
    requiredHeaders: {
      "Content-Type": "text/csv",
      "x-amz-checksum-sha256": checksumSha256B64,
    },
    expiresIn,
  };
};

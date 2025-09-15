const {S3Client} = require("@aws-sdk/client-s3");
const {DynamoDBClient} = require("@aws-sdk/client-dynamodb");
const axios = require("axios");
const fs = require('fs');
const crypto = require('crypto');

class SafeStorageClient {

    constructor(safeStorageUrl) {
        this._safeStorageUrl = safeStorageUrl;
    }

    /**
     * Calculates the SHA-256 in base64 of a JSON content
     * @param {string|object} jsonContent - JSON content (string or object)
     * @returns {string}
     */
    computeSha256Base64(jsonContent) {
        let content;
        if (typeof jsonContent === 'object') {
            content = JSON.stringify(jsonContent);
        } else {
            content = jsonContent;
        }

        const buffer = Buffer.from(content, 'utf8');
        const hash = crypto.createHash('sha256').update(buffer).digest('base64');
        return hash;
    }

    /**
     * Performs a POST to upload the file metadata to Safe Storage
     * @param {string} sha256Base64 - SHA-256 in base64
     * @returns {Promise<any>}
     */
    async uploadFileMetadata(sha256Base64) {
        const url = `${this._safeStorageUrl}/safe-storage/v1/files`;
        const headers = {
            "x-pagopa-safestorage-cx-id": "pn-portfat-in",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-checksum-value": sha256Base64
        };
        const data = {
            contentType: "application/json",
            documentType: "PN_SERVICE_ORDER",
            status: "SAVED"
        };

        try {
            const response = await axios.post(url, data, { headers });
            console.log(`POST successful for checksum: ${sha256Base64}`);
            return response.data;
        } catch (error) {
            console.error(`Error during POST for checksum ${sha256Base64}:`, error.message);
            throw error;
        }
    }

    /**
     * Performs a PUT to upload the file
     * @param {string} uploadUrl - Presigned URL for upload
     * @param {string} secret - Secret to include in headers
     * @param {string|object} jsonContent - JSON content to upload
     * @param {string} sha256Base64 - SHA-256 in base64
     * @returns {Promise<any>}
     */
    async uploadFileContent(uploadUrl, secret, jsonContent, sha256Base64) {
        let bodyContent;
        if (typeof jsonContent === 'object') {
            bodyContent = JSON.stringify(jsonContent, null, 2);
        } else {
            bodyContent = jsonContent;
        }

        const headers = {
            'x-amz-checksum-sha256': sha256Base64,
            'x-amz-meta-secret': secret,
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(bodyContent, 'utf8')
        };

        try {
            console.log(`Uploading file content to: ${uploadUrl}`);

            const response = await axios.put(uploadUrl, bodyContent, {
                headers,
                maxContentLength: Infinity,
                maxBodyLength: Infinity
            });

            console.log(`PUT successful for file upload - Status: ${response.status}`);
            return response;
        } catch (error) {
            console.error(`Error during PUT for file upload:`, error.message);
            if (error.response) {
                console.error(`Response status: ${error.response.status}`);
                console.error(`Response data:`, error.response.data);
            }
            throw error;
        }
    }

    /**
     * Processes a single JSON file
     * @param {string} filePath - Path of the JSON file
     * @returns {Promise<object>}
     */
    async processJsonFile(filePath) {
        try {
            console.log(`Processing file: ${filePath}`);

            const fileContent = fs.readFileSync(filePath, 'utf8');
            const jsonData = JSON.parse(fileContent);

            // Evaluate SHA-256
            const contentToSend = JSON.stringify(jsonData, null, 2);
            const sha256Base64 = this.computeSha256Base64(contentToSend);
            console.log(`SHA-256 calculated: ${sha256Base64}`);

            // POST to retrieve upload metadata
            const uploadMetadata = await this.uploadFileMetadata(sha256Base64);

            // PUT to upload the file
            await this.uploadFileContent(
                uploadMetadata.uploadUrl,
                uploadMetadata.secret,
                jsonData,
                sha256Base64
            );

            return {
                filePath,
                key: uploadMetadata.key,
                sha256: sha256Base64,
                success: true
            };

        } catch (error) {
            console.error(`Error processing file ${filePath}:`, error.message);
            return {
                filePath,
                error: error.message,
                success: false
            };
        }
    }
  }

exports.SafeStorageClient = SafeStorageClient;
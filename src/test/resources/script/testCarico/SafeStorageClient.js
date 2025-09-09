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
     * Calcola lo SHA-256 in base64 di un contenuto JSON
     * @param {string|object} jsonContent - Contenuto JSON (stringa o oggetto)
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
     * Effettua la POST per caricare i metadati del file su Safe Storage
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
     * Effettua la PUT per caricare il file effettivo
     * @param {string} uploadUrl - URL presigned per l'upload
     * @param {string} secret - Secret da includere negli headers
     * @param {string|object} jsonContent - Contenuto JSON da caricare
     * @param {string} sha256Base64 - SHA-256 in base64
     * @returns {Promise<any>}
     */
    async uploadFileContent(uploadUrl, secret, jsonContent, sha256Base64) {
        // Prepara il contenuto da inviare nel body
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
            console.log(`Body content length: ${Buffer.byteLength(bodyContent, 'utf8')} bytes`);

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
     * Processa un singolo file JSON
     * @param {string} filePath - Path del file JSON
     * @returns {Promise<object>}
     */
    async processJsonFile(filePath) {
        try {
            console.log(`Processing file: ${filePath}`);

            // Leggi il file JSON
            const fileContent = fs.readFileSync(filePath, 'utf8');
            const jsonData = JSON.parse(fileContent);

            // Calcola SHA-256 sul contenuto che verrà effettivamente inviato
            const contentToSend = JSON.stringify(jsonData, null, 2);
            const sha256Base64 = this.computeSha256Base64(contentToSend);
            console.log(`SHA-256 calculated: ${sha256Base64}`);

            // Effettua la POST per ottenere i metadati di upload
            const uploadMetadata = await this.uploadFileMetadata(sha256Base64);

            // Effettua la PUT per caricare il file con il contenuto nel body
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

    /**
     * Metodo helper per verificare che lo SHA-256 del contenuto raw corrisponda
     * @param {string} rawContent - Contenuto raw da verificare
     * @param {string} expectedSha256 - SHA-256 atteso
     * @returns {boolean}
     */
    validateBodyChecksum(rawContent, expectedSha256) {
        const computedSha256 = this.computeSha256Base64(rawContent);
        return computedSha256 === expectedSha256;
    }
}

// Aggiungi import per path se non presente
const path = require('path');

// Controllo per impedire esecuzione diretta
if (require.main === module) {
    console.error('SafeStorageClient non può essere eseguito direttamente!');
    console.log('Utilizza invece: node index.js');
    process.exit(1);
}

exports.SafeStorageClient = SafeStorageClient;
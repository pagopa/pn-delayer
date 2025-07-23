'use strict';
const axios = require('axios');

const SAFE_STORAGE_URL = process.env.SAFE_STORAGE_URL;
const CX_ID = process.env.CX_ID;

if (!SAFE_STORAGE_URL || !CX_ID) {
    /* eslint-disable no-console */
    console.warn('SAFE_STORAGE_URL or CX_ID env variables are missing – SafeStorage client will not work properly.');
}

/**
 * Retrieves JSON file content from Safe Storage given its fileKey.
 * @param {string} fileKey
 * @returns {Promise<object>}
 */
async function downloadJson(fileKey) {
    const metaUrl = `${SAFE_STORAGE_URL}/safe-storage/v1/files/${encodeURIComponent(fileKey)}`;
  const metaResp = await axios.get(metaUrl, {
    headers: { 'x-pagopa-safestorage-cx-id': CX_ID }
  });

  const presignedUrl = metaResp.data?.download?.url;
  if (!presignedUrl) {
    throw new Error('File metadata retrieved but download URL not present (file may be cold).');
  }

  const fileResp = await axios.get(presignedUrl, { responseType: 'json' });
  return fileResp.data;
}

module.exports = { downloadJson };

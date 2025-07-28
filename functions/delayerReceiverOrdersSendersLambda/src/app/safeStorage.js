'use strict';
const axios = require('axios');

const PN_SAFESTORAGE_URL = process.env.PN_SAFESTORAGE_URL;
const PN_SAFESTORAGE_CXID = process.env.PN_SAFESTORAGE_CXID;

if (!PN_SAFESTORAGE_URL || !PN_SAFESTORAGE_CXID) {
    /* eslint-disable no-console */
    console.warn('PN_SAFESTORAGE_URL or PN_SAFESTORAGE_CXID env variables are missing – SafeStorage client will not work properly.');
}

/**
 * Retrieves JSON file content from Safe Storage given its fileKey.
 * @param {string} fileKey
 * @returns {Promise<object>}
 */
async function downloadJson(fileKey) {
  const metaUrl = `${PN_SAFESTORAGE_URL}safe-storage/v1/files/${encodeURIComponent(fileKey)}`;
  console.info(`[SAFE] ▶︎ Fetching metadata for fileKey="${fileKey}" – ${metaUrl}`);
  const metaResp = await axios.get(metaUrl, {
    headers: { 'x-pagopa-safestorage-cx-id': PN_SAFESTORAGE_CXID }
  });

  const presignedUrl = metaResp.data?.download?.url;
  if (!presignedUrl) {
    throw new Error('File metadata retrieved but download URL not present (file may be cold).');
  }

  console.debug('[SAFE] Presigned URL obtained – downloading JSON…');
  const fileResp = await axios.get(presignedUrl, { responseType: 'json' });
  return fileResp.data;
}

module.exports = { downloadJson };

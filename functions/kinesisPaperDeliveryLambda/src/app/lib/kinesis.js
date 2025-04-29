const { Buffer } = require("node:buffer");
const { gunzipSync } = require("node:zlib");

function myGunzip(buffer) {
  return gunzipSync(buffer);
}

function decodePayload(b64Str) {
  let parsedJson = undefined;
  if(b64Str) {
    const payloadBuf = Buffer.from(b64Str, "base64");
    try {
      parsedJson = JSON.parse(payloadBuf.toString("utf8"));
    } catch (err) {
      const uncompressedBuf = myGunzip(payloadBuf);
      parsedJson = JSON.parse(uncompressedBuf.toString("utf8"));
    }

    return parsedJson;
  }
}

exports.extractKinesisData = function (kinesisEvent) {
  return kinesisEvent?.Records?.map((rec) => {
      const decodedPayload = decodePayload(rec?.kinesis?.data);
      if (decodedPayload?.detail?.body) {
        return {
          kinesisSeqNumber: rec.kinesis.sequenceNumber,
          ...decodedPayload.detail.body
        };
      }
      return null;
    })
    .filter(item => item !== null);
};

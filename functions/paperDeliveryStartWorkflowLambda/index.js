const { handleEvent } = require("./src/app/eventHandler.js");

async function handler() {
  return handleEvent();
}

exports.handler = handler;

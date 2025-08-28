const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");

const client = new SNSClient();

async function publishToSnsTopic(topicArn, message, subject) {
  const response = await client.send(new PublishCommand({
    TopicArn: topicArn,
    Message: message,
    Subject: subject,
  }));
  return response.MessageId;
}

module.exports = { publishToSnsTopic };

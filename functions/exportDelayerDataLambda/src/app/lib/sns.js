const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");
const { fromIni } = require("@aws-sdk/credential-provider-ini");

/*function awsClientCfg(profile) {
  const self = this;
  return {
    region: "eu-south-1",
    credentials: fromIni({
      profile: profile,
    })
  }
}

const client = new SNSClient(awsClientCfg('sso_pn-core-dev'));
*/
const client = new SNSClient();

async function publishToSnsTopic(topicArn, message, subject) {

  await client.send(new PublishCommand({
    TopicArn: topicArn,
    Message: message,
    Subject: subject,
  }));
}

module.exports = { publishToSnsTopic }
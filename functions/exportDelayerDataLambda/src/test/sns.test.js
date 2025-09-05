const { expect } = require('chai');
const proxyquire = require('proxyquire');

class MockPublishCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockSNSClient {
  async send(command) {
    this.lastCommand = command;
    return { MessageId: 'mock-message-id' };
  }
}

describe('publishToSnsTopic', () => {
  const { publishToSnsTopic } = proxyquire('../app/lib/sns', {
    '@aws-sdk/client-sns': {
      SNSClient: MockSNSClient,
      PublishCommand: MockPublishCommand
    }
  });

  it('send SNS message and retrieve MessageId', async () => {
    const topicArn = 'arn:aws:sns:eu-south-1:123456789012:my-topic';
    const message = 'Messaggio di test';
    const subject = 'Subject di test';

    const messageId = await publishToSnsTopic(topicArn, message, subject);

    expect(messageId).to.equal('mock-message-id');
  });
});

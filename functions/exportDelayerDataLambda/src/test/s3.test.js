const { expect } = require('chai');
const proxyquire = require('proxyquire');

class MockPutObjectCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockGetObjectCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockS3Client {
  async send(command) {
    this.lastCommand = command;
    return {
      ETag: '"mock-etag"',
      VersionId: 'mock-version-id'
    };
  }
}

// mock di getSignedUrl
const mockGetSignedUrl = async (client, command, options) => {
  return `https://mock-s3-url/${command.input.Bucket}/${command.input.Key}?exp=${options.expiresIn}`;
};

describe('s3 library', () => {
  // carica il modulo con i mock
  const { putObject, getpresigneUrlObject } = proxyquire('../app/lib/s3', {
    '@aws-sdk/client-s3': {
      S3Client: MockS3Client,
      PutObjectCommand: MockPutObjectCommand,
      GetObjectCommand: MockGetObjectCommand
    },
    '@aws-sdk/s3-request-presigner': {
      getSignedUrl: mockGetSignedUrl
    }
  });

  it('s3 object upload with mocked response', async () => {
    const bucket = 'test-bucket';
    const fileName = 'test-file.txt';
    const fileBody = 'Contenuto del file';

    const response = await putObject(bucket, fileName, fileBody);

    expect(response).to.have.property('ETag', '"mock-etag"');
    expect(response).to.have.property('VersionId', 'mock-version-id');
  });

  it('generate mocked presignedUrl', async () => {
    process.env.SIGN_URL_EXPIRATION = '3600';
    const bucket = 'test-bucket';
    const fileName = 'test-file.txt';

    const url = await getpresigneUrlObject(bucket, fileName);

    expect(url).to.include(`https://mock-s3-url/${bucket}/${fileName}`);
  });
});

const { fromSSO } = require('@aws-sdk/credential-provider-sso');
const { fromIni } = require('@aws-sdk/credential-provider-ini');
const { fromTemporaryCredentials } = require('@aws-sdk/credential-providers');

class AwsAuthClient {
  /**
   * Return valid AWS credentials using sso profile to perform authentication
   *
   * @param profile   profile to use during authentication
   * @param isLocal   if local return mocked credentials
   * @param roleArn  if provided, assume the specified role using the profile as source
   *
   * @return AWS temporary credentials
   * */
  async getCredentials(profile, isLocal, noSSO) {
    if (isLocal) {
      return {
        accessKeyId: 'local',
        secretAccessKey: 'local',
        sessionToken: 'local',
      };
    }

    if (noSSO) {
      return fromIni({ profile: 'dev' })();
    }
    return fromSSO({ profile })();
  }
}

exports.AwsAuthClient = AwsAuthClient;

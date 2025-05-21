const { fromSSO } = require('@aws-sdk/credential-provider-sso');

class AwsAuthClient {
  /**
   * Return valid AWS credentials using sso profile to perform authentication
   *
   * @param profile   profile to use during authentication
   * @param isLocal   if local return mocked credentials
   *
   * @return AWS temporary credentials
   * */
  async ssoCredentials(profile, isLocal) {
    return isLocal
      ? {
          accessKeyId: 'local',
          secretAccessKey: 'local',
          sessionToken: 'local',
        }
      : fromSSO({ profile: profile })();
  }
}

exports.AwsAuthClient = AwsAuthClient;

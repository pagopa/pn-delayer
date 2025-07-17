package it.pagopa.pn.delayer.config.springbootcfg;

import it.pagopa.pn.commons.configs.aws.AwsConfigs;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;

import java.net.URI;

@Configuration
public class AwsLambdaConfig {

    private final AwsConfigs props;

    public AwsLambdaConfig(AwsConfigs props) {
        this.props = props;
    }

    @Bean
    public LambdaClient lambdaClient() {
        return configureBuilder(LambdaClient.builder());
    }

    private LambdaClient configureBuilder(LambdaClientBuilder builder) {
        if (this.props != null) {
            String profileName = this.props.getProfileName();
            if (StringUtils.isNotBlank(profileName)) {
                builder.credentialsProvider(ProfileCredentialsProvider.create(profileName));
            }

            String regionCode = this.props.getRegionCode();
            if (StringUtils.isNotBlank(regionCode)) {
                builder.region(Region.of(regionCode));
            }

            String endpointUrl = this.props.getEndpointUrl();
            if (StringUtils.isNotBlank(endpointUrl)) {
                builder.endpointOverride(URI.create(endpointUrl));
            }
        }

        return builder.build();
    }
}

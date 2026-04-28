package it.pagopa.pn.delayer.config.springbootcfg;

import it.pagopa.pn.commons.configs.aws.AwsConfigs;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@ConfigurationProperties("aws")
@Profile("!aws")
public class AwsConfigsActivation extends AwsConfigs {
}

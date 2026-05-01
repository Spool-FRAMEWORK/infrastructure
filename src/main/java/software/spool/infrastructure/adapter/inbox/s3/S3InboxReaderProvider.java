package software.spool.infrastructure.adapter.inbox.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.spool.core.port.inbox.InboxReader;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.net.URI;

@SpoolPlugin(InboxReaderProvider.class)
public class S3InboxReaderProvider implements InboxReaderProvider {
    @Override
    public String name() {
        return "S3";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("region") &&
                configuration.has("bucket") &&
                configuration.has("endpoint");
    }

    @Override
    public InboxReader create(PluginConfiguration configuration) {
        return new S3InboxReader(
                buildS3Client(configuration.require("region"), configuration.require("endpoint")),
                configuration.require("bucket"));
    }

    private S3Client buildS3Client(String region, String endpoint) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(region));

        if (endpoint != null && !endpoint.isBlank()) {
            builder
                    .endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")
                    ))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build());
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }
}

package software.spool.infrastructure.adapter.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthStatus;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

class S3HealthProbeTest {
    private static final String BUCKET = "spool-datalake";

    @Test
    void probe_bucketThatAnswers_isHealthyAndAsksForASingleKey() {
        InMemoryS3Client s3Client = new InMemoryS3Client();

        HealthCheck check = new S3HealthProbe(s3Client, BUCKET).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.HEALTHY);
        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isEqualTo(1);
    }

    @Test
    void probe_bucketThatCannotBeListed_isUnhealthyAndNamesTheBucket() {
        HealthCheck check = new S3HealthProbe(clientWithoutTheBucket(), BUCKET).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.reason()).contains(BUCKET).contains("The specified bucket does not exist");
    }

    @Test
    void provider_supportsAPieceWithBucketRegionAndEndpoint() {
        S3HealthProbeProvider provider = new S3HealthProbeProvider();

        assertThat(provider.name()).isEqualTo("S3");
        assertThat(provider.supports(PluginConfiguration.builder().with("bucket", BUCKET).with("region", "us-east-1")
                .with("endpoint", "http://localhost:4566").build())).isTrue();
        assertThat(provider.supports(PluginConfiguration.builder().with("bucket", BUCKET).build())).isFalse();
    }

    private static S3Client clientWithoutTheBucket() {
        return new S3Client() {
            @Override
            public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
                throw NoSuchBucketException.builder().message("The specified bucket does not exist").build();
            }

            @Override
            public String serviceName() {
                return "s3";
            }

            @Override
            public void close() {
            }
        };
    }
}

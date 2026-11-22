package software.spool.infrastructure.adapter.s3;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthStatus;

import java.net.HttpURLConnection;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class S3HealthProbeLocalStackTest {
    private static final String BUCKET = "spool-health-probe-test";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";

    private static S3Client s3;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(isReachable(), "LocalStack not available: run docker compose -f .docker/docker-compose.yml up -d localstack");
        s3 = S3Client.builder()
                .region(Region.of(REGION))
                .endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        try {
            s3.createBucket(request -> request.bucket(BUCKET));
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
        }
    }

    @Test
    void probe_existingBucket_isHealthy() {
        assertThat(new S3HealthProbe(s3, BUCKET).probe().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void probe_bucketThatDoesNotExist_isUnhealthyAndNamesIt() {
        HealthCheck check = new S3HealthProbe(s3, "spool-health-probe-missing").probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.reason()).contains("spool-health-probe-missing");
    }

    private static boolean isReachable() {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(ENDPOINT + "/_localstack/health").toURL().openConnection();
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            return connection.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }
}

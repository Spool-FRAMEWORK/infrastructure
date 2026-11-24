package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.UUID;

@Tag("docker")
class S3DataMartStoreLocalStackTest extends DataMartStoreContract {
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";

    private DataMartStore store;

    @BeforeEach
    void setUp() {
        Assumptions.assumeTrue(isReachable(), "LocalStack not available: run docker compose -f .docker/docker-compose.yml up -d localstack");
        S3Client s3 = S3Client.builder()
                .region(Region.of(REGION))
                .endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        String bucket = "spool-datamart-store-" + UUID.randomUUID();
        s3.createBucket(request -> request.bucket(bucket));
        store = new S3DataMartStore(s3, bucket);
    }

    @Override
    DataMartStore store() {
        return store;
    }

    private static boolean isReachable() {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(ENDPOINT + "/_localstack/health").toURL().openConnection();
            connection.setConnectTimeout(1000);
            return connection.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }
}

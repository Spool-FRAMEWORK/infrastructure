package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.S3ObjectKeys;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Tag("docker")
class S3DataMartReplacementLocalStackTest extends DataMartReplacementContract {
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";

    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private S3Client s3;
    private String bucket;
    private S3DataMartWriter writer;

    @BeforeEach
    void setUp() {
        Assumptions.assumeTrue(isReachable(), "LocalStack not available: run docker compose -f .docker/docker-compose.yml up -d localstack");
        s3 = S3Client.builder()
                .region(Region.of(REGION))
                .endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        bucket = "spool-datamart-rounds-" + UUID.randomUUID();
        s3.createBucket(request -> request.bucket(bucket));
        writer = new S3DataMartWriter(s3, bucket, RecordSerializerFactory.record(), concurrency);
    }

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Override
    DataMartWriter writer() {
        return writer;
    }

    @Override
    DataMartWriter writerWith(MountRoundSettings settings) {
        return new S3DataMartWriter(s3, bucket, RecordSerializerFactory.record(), concurrency, settings);
    }

    @Override
    PartitionedRecord<?> result(PartitionKey partition, String content) {
        return new PartitionedRecord<>(partition, GenericRecord.of(Map.of("value", content)));
    }

    @Override
    String pathOf(MountTarget target, PartitionedRecord<?> result) {
        return DataMartObject.of(target, result, RecordSerializerFactory.record()).getFirst().path();
    }

    @Override
    List<String> allObjects() {
        return S3ObjectKeys.under(s3, bucket, "").stream().sorted().toList();
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

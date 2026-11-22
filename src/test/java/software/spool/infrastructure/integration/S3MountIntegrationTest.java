package software.spool.infrastructure.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.infrastructure.adapter.dataLake.s3.S3DataLakeWriter;
import software.spool.infrastructure.adapter.dataLake.s3.S3PartitionedReader;
import software.spool.infrastructure.adapter.datamart.S3DataMartWriter;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class S3MountIntegrationTest {
    private static final String DATA_LAKE = "spool-mount-datalake-test";
    private static final String DATA_MART = "spool-mount-datamart-test";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final PartitionKey PAYMENTS = PartitionKey.ofEntries().with("source", "payments").with("eventType", "Payment").build();
    private static final PartitionKey READ = PartitionKey.ofEntriesWithoutDate().with("source", "payments").build();
    private static final PartitionKey TOTAL = PartitionKey.ofEntriesWithoutDate().with("scope", "all").build();

    private static S3Client s3;
    private static BoundedConcurrency concurrency;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(isReachable(), "LocalStack not available: run docker compose -f .docker/docker-compose.yml up -d localstack");
        s3 = S3Client.builder()
                .region(Region.of(REGION))
                .endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        createBucket(DATA_LAKE);
        createBucket(DATA_MART);
        concurrency = BoundedConcurrency.withThreads(4);
    }

    @AfterAll
    static void closeConcurrency() {
        if (concurrency != null) concurrency.close();
    }

    @AfterEach
    void cleanBuckets() {
        Stream.of(DATA_LAKE, DATA_MART).forEach(bucket -> s3.listObjectsV2(request -> request.bucket(bucket)).contents()
                .stream().map(S3Object::key)
                .forEach(key -> s3.deleteObject(request -> request.bucket(bucket).key(key))));
    }

    @Test
    void mount_bronzeOfAnS3DataLake_isPromotedToSilverAndAggregatedIntoGoldOfAnS3DataMart() {
        new S3DataLakeWriter(s3, DATA_LAKE).write(List.of(new PartitionedEnvelope(PAYMENTS, payment("payment-1", 10)),
                new PartitionedEnvelope(PAYMENTS, payment("payment-2", 25)))).toList();
        S3PartitionedReader dataLake = new S3PartitionedReader(s3, DATA_LAKE, concurrency);

        new S3DataMartWriter(s3, DATA_LAKE, concurrency).write(MountTarget.transformation("payments", READ),
                dataLake.read(MountTarget.transformation("payments", READ)).map(record -> record));
        List<PartitionedRecord<GenericRecord>> silver = dataLake.read(MountTarget.aggregation("payments-total", READ)).toList();
        long total = silver.stream().mapToLong(record -> record.record().getNested("payload").getLong("amount")).sum();
        new S3DataMartWriter(s3, DATA_MART, concurrency).write(MountTarget.aggregation("payments-total", READ),
                Stream.of(new PartitionedRecord<>(TOTAL, Map.of("totalAmount", total))));

        assertThat(silver).hasSize(2);
        assertThat(keysOf(DATA_LAKE)).filteredOn(key -> key.startsWith("silver/payments/")).hasSize(2);
        assertThat(keysOf(DATA_MART)).singleElement().satisfies(key -> {
            assertThat(key).startsWith("gold/payments-total/scope=all/");
            assertThat(PayloadDeserializerFactory.json().asMap().deserialize(s3.getObjectAsBytes(request ->
                    request.bucket(DATA_MART).key(key)).asByteArray())).containsEntry("totalAmount", 35);
        });
    }

    private static List<String> keysOf(String bucket) {
        return s3.listObjectsV2(request -> request.bucket(bucket)).contents().stream().map(S3Object::key).toList();
    }

    private static void createBucket(String bucket) {
        try {
            s3.createBucket(request -> request.bucket(bucket));
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
        }
    }

    private static Envelope payment(String id, long amount) {
        byte[] payload = ("{\"id\":\"" + id + "\",\"amount\":" + amount + "}").getBytes(StandardCharsets.UTF_8);
        return new Envelope(IdempotencyKey.of(id), new EventMetadata().set(EventMetadataKey.SOURCE, "payments"),
                MediaType.of("application/json"), payload, EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
    }

    private static boolean isReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(URI.create(ENDPOINT).getHost(), URI.create(ENDPOINT).getPort()), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

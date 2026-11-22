package software.spool.infrastructure.adapter.dataLake.s3;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class S3PartitionedReaderLocalStackTest {
    private static final String BUCKET = "spool-datalake-reader-test";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final PartitionKey YEAR = PartitionKey.ofEntriesWithoutDate().with("year", "2026").build();
    private static final PartitionKey PAYMENTS = PartitionKey.ofEntriesWithoutDate().with("year", "2026")
            .with("source", "payments").build();
    private static final PartitionKey REPORTS = PartitionKey.ofEntriesWithoutDate().with("year", "2026")
            .with("source", "boletín").build();
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final byte[] PDF = {0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37, (byte) 0xe2, (byte) 0xe3};

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
        try {
            s3.createBucket(request -> request.bucket(BUCKET));
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
        }
        concurrency = BoundedConcurrency.withThreads(4);
    }

    @AfterAll
    static void closeConcurrency() {
        if (concurrency != null) concurrency.close();
    }

    @AfterEach
    void cleanBucket() {
        s3.listObjectsV2(request -> request.bucket(BUCKET)).contents().stream().map(S3Object::key)
                .forEach(key -> s3.deleteObject(request -> request.bucket(BUCKET).key(key)));
    }

    @Test
    void read_partitionWithEnvelopesInEveryFormat_givesEachOneWithItsPayloadAndItsPartition() {
        Envelope payment = envelope("payment-1", "payments", MediaType.of("application/json"),
                "{\"id\":\"payment-1\",\"amount\":10}".getBytes(StandardCharsets.UTF_8));
        Envelope report = envelope("report-1", "Boletín Oficial del Estado " + "x".repeat(3000), MediaType.of("application/pdf"), PDF);
        Envelope former = envelope("payment-0", "payments", MediaType.of("application/json"),
                "{\"id\":\"payment-0\",\"amount\":5}".getBytes(StandardCharsets.UTF_8));
        new S3DataLakeWriter(s3, BUCKET).write(List.of(new PartitionedEnvelope(PAYMENTS, payment),
                new PartitionedEnvelope(REPORTS, report))).toList();
        s3.putObject(request -> request.bucket(BUCKET).key(S3DataLakeLayout.formerKeyOf(PAYMENTS, former.idempotencyKey())),
                RequestBody.fromBytes(RecordSerializerFactory.record().serialize(former)));

        Map<String, PartitionedRecord<GenericRecord>> read = new S3PartitionedReader(s3, BUCKET, concurrency)
                .read(MountTarget.transformation("payments", YEAR))
                .collect(Collectors.toMap(record -> record.record().getNested("idempotencyKey").getString("value"),
                        Function.identity()));

        assertThat(read).containsOnlyKeys("payment-0", "payment-1", "report-1");
        assertThat(read.get("payment-1").partitionKey()).isEqualTo(PAYMENTS);
        assertThat(read.get("payment-1").record().getNested("payload").getLong("amount")).isEqualTo(10L);
        assertThat(read.get("payment-0").record().getNested("payload").getLong("amount")).isEqualTo(5L);
        assertThat(read.get("report-1").partitionKey()).isEqualTo(REPORTS);
        assertThat(read.get("report-1").record().get("payload")).isEqualTo(PDF);
    }

    private static Envelope envelope(String key, String source, MediaType mediaType, byte[] payload) {
        return new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, source), mediaType,
                payload, EnvelopeStatus.PERSISTED, 0, NOW, NOW);
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

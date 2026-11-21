package software.spool.infrastructure.adapter.dataLake.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class S3DataLakeLocalStackTest {
    private static final String BUCKET = "spool-datalake-test";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "boletín").build();
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final byte[] PDF = {0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37, (byte) 0xe2, (byte) 0xe3};

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

    @AfterEach
    void cleanBucket() {
        s3.listObjectsV2(request -> request.bucket(BUCKET)).contents().stream().map(S3Object::key)
                .forEach(key -> s3.deleteObject(request -> request.bucket(BUCKET).key(key)));
    }

    @Test
    void write_pdfWithAHeaderThatHasNonAsciiCharacters_keepsThePdfAndItsHeaderInOneObject() {
        Envelope envelope = envelope("informe-1", "Boletín Oficial del Estado");

        new S3DataLakeWriter(s3, BUCKET).write(List.of(new PartitionedEnvelope(PARTITION, envelope))).toList();

        ResponseBytes<GetObjectResponse> object = s3.getObjectAsBytes(request -> request.bucket(BUCKET)
                .key(S3DataLakeLayout.payloadKeyOf(PARTITION, envelope.header())));
        assertThat(object.asByteArray()).isEqualTo(PDF);
        assertThat(object.response().contentType()).isEqualTo("application/pdf");
        assertThat(S3EnvelopeHeaderMetadata.headerIn(object.response().metadata())).contains(envelope.header());
    }

    @Test
    void write_headerTooBigForTheObjectMetadata_isAcceptedAndRemovedWithItsPayload() {
        Envelope envelope = envelope("informe-2", "x".repeat(3000));

        List<IdempotencyKey> written = new S3DataLakeWriter(s3, BUCKET)
                .write(List.of(new PartitionedEnvelope(PARTITION, envelope))).toList();
        new S3DataLakeEnvelopeRemover(s3, BUCKET).remove(PARTITION, envelope.idempotencyKey());

        assertThat(written).containsExactly(envelope.idempotencyKey());
        assertThat(s3.listObjectsV2(request -> request.bucket(BUCKET)).contents()).isEmpty();
    }

    private static Envelope envelope(String key, String source) {
        return new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, source),
                MediaType.of("application/pdf"), PDF, EnvelopeStatus.PERSISTED, 0, NOW, NOW);
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

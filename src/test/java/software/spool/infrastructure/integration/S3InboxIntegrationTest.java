package software.spool.infrastructure.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.infrastructure.adapter.inbox.s3.*;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("docker")
class S3InboxIntegrationTest {

    private static final String BUCKET   = "spool-inbox-test";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION   = "us-east-1";

    private static S3Client s3;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(isReachable("localhost", 4566),
                "LocalStack not available — run: docker compose -f .docker/docker-compose.yml up -d");
        s3 = buildS3Client();
        try {
            s3.createBucket(b -> b.bucket(BUCKET));
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
        }
    }

    @AfterEach
    void cleanBucket() {
        ListObjectsV2Response listing = s3.listObjectsV2(b -> b.bucket(BUCKET));
        listing.contents().forEach(obj ->
                s3.deleteObject(b -> b.bucket(BUCKET).key(obj.key())));
    }

    // ── Writer ──────────────────────────────────────────────────────────────

    @Test
    void receive_newKey_writesObjectToS3() throws Exception {
        S3InboxWriter writer = new S3InboxWriter(s3, BUCKET);

        Envelope envelope = mock(Envelope.class);
        IdempotencyKey key = mock(IdempotencyKey.class);
        EventMetadata metadata = mock(EventMetadata.class);

        when(key.value()).thenReturn("new-key-123");
        when(envelope.idempotencyKey()).thenReturn(key);
        when(envelope.status()).thenReturn(EnvelopeStatus.CAPTURED);
        when(envelope.metadata()).thenReturn(metadata);
        when(envelope.payload()).thenReturn(new byte[0]);
        when(envelope.retries()).thenReturn(0);
        when(envelope.capturedAt()).thenReturn(Instant.EPOCH);

        IdempotencyKey result = writer.receive(envelope);

        assertThat(result.value()).isEqualTo("new-key-123");
        assertThat(objectExists("inbox/CAPTURED/new-key-123")).isTrue();
    }

    @Test
    void receive_duplicateKey_throwsDuplicateEventException() throws Exception {
        writeFixture("duplicate-key", EnvelopeStatus.CAPTURED);

        S3InboxWriter writer = new S3InboxWriter(s3, BUCKET);

        Envelope envelope = mock(Envelope.class);
        IdempotencyKey key = mock(IdempotencyKey.class);

        when(key.value()).thenReturn("duplicate-key");
        when(envelope.idempotencyKey()).thenReturn(key);
        when(envelope.status()).thenReturn(EnvelopeStatus.CAPTURED);

        assertThatThrownBy(() -> writer.receive(envelope))
                .isInstanceOf(DuplicateEventException.class);
    }

    // ── Reader ──────────────────────────────────────────────────────────────

    @Test
    void findById_existingKey_returnsNonEmptyOptional() throws Exception {
        writeFixture("find-key", EnvelopeStatus.CAPTURED);

        S3InboxReader reader = new S3InboxReader(s3, BUCKET);

        Optional<Envelope> result = reader.findById(keyOf("find-key"));

        assertThat(result).isPresent();
    }

    @Test
    void findByStatus_returnsAllForStatus() throws Exception {
        writeFixture("key-a", EnvelopeStatus.CAPTURED);
        writeFixture("key-b", EnvelopeStatus.CAPTURED);

        S3InboxReader reader = new S3InboxReader(s3, BUCKET);

        Collection<Envelope> result = reader.findByStatus(EnvelopeStatus.CAPTURED);

        assertThat(result).hasSize(2);
    }

    @Test
    void findById_missingKey_returnsEmpty() throws Exception {
        S3InboxReader reader = new S3InboxReader(s3, BUCKET);

        Optional<Envelope> result = reader.findById(keyOf("no-such-key"));

        assertThat(result).isEmpty();
    }

    // ── Updater ─────────────────────────────────────────────────────────────

    @Test
    void update_movesObjectToNewStatus() throws Exception {
        EnvelopeStatus initial = EnvelopeStatus.CAPTURED;
        EnvelopeStatus target  = nextStatus(initial);
        writeFixture("update-key", initial);

        S3InboxUpdater updater = new S3InboxUpdater(s3, BUCKET);

        Collection<Envelope> result = updater.update(List.of(keyOf("update-key")), target);

        assertThat(result).hasSize(1);
        assertThat(objectExists("inbox/" + initial.name() + "/update-key")).isFalse();
        assertThat(objectExists("inbox/" + target.name()  + "/update-key")).isTrue();
    }

    // ── Remover ─────────────────────────────────────────────────────────────

    @Test
    void remove_deletesObjectFromBucket() throws Exception {
        writeFixture("remove-key", EnvelopeStatus.CAPTURED);

        S3InboxRemover remover = new S3InboxRemover(s3, BUCKET);

        Collection<Envelope> result = remover.remove(List.of(keyOf("remove-key")));

        assertThat(result).hasSize(1);
        assertThat(objectExists("inbox/CAPTURED/remove-key")).isFalse();
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static void writeFixture(String key, EnvelopeStatus status) throws Exception {
        String s3Key = "inbox/" + status.name() + "/" + key;
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Map<String, Object> dto = new LinkedHashMap<>();
        dto.put("idempotencyKey", key);
        dto.put("metadata",       "{}".getBytes(StandardCharsets.UTF_8));
        dto.put("payload",        new byte[0]);
        dto.put("status",         status.name());
        dto.put("retries",        0);
        dto.put("capturedAt",     Instant.EPOCH);
        byte[] bytes = mapper.writeValueAsBytes(dto);
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET).key(s3Key).contentType("application/json")
                        .build(),
                RequestBody.fromBytes(bytes)
        );
    }

    private static boolean objectExists(String s3Key) {
        try {
            s3.headObject(b -> b.bucket(BUCKET).key(s3Key));
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    private static IdempotencyKey keyOf(String value) {
        IdempotencyKey key = mock(IdempotencyKey.class);
        when(key.value()).thenReturn(value);
        return key;
    }

    private static EnvelopeStatus nextStatus(EnvelopeStatus current) {
        EnvelopeStatus[] values = EnvelopeStatus.values();
        for (int i = 0; i < values.length - 1; i++) {
            if (values[i] == current) return values[i + 1];
        }
        throw new IllegalStateException("No next status after " + current);
    }

    private static S3Client buildS3Client() {
        return S3Client.builder()
                .region(Region.of(REGION))
                .endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    private static boolean isReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

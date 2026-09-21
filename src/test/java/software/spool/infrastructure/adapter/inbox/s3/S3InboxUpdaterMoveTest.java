package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class S3InboxUpdaterMoveTest {

    private static final String BUCKET = "spool-inbox-test";
    private static final String KEY = "moved-key";
    private static final Instant CAPTURED_AT = Instant.now().minus(Duration.ofDays(2));

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);
    private final S3InboxUpdater updater = new S3InboxUpdater(s3Client, BUCKET);
    private final S3InboxReader reader = new S3InboxReader(s3Client, BUCKET);

    @Test
    void update_toPersisted_rewritesTheStoredStatusAndUpdatedAt() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        JsonNode stored = new ObjectMapper().readTree(stored("inbox/PERSISTED/" + KEY));
        assertThat(stored.get("status").asText()).isEqualTo("PERSISTED");
        assertThat(stored.hasNonNull("updatedAt")).isTrue();
    }

    @Test
    void update_toPersisted_movesTheObjectOutOfTheOldFolder() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        assertThat(s3Client.keys()).containsExactly("inbox/PERSISTED/" + KEY);
    }

    @Test
    void update_toPersisted_keepsThePayloadAndTheCaptureDate() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        Optional<Envelope> found = reader.findById(IdempotencyKey.of(KEY));
        assertThat(found).isPresent();
        assertThat(found.get().payload()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
        assertThat(found.get().capturedAt()).isBefore(Instant.now().minus(Duration.ofDays(1)));
    }

    @Test
    void update_toPersisted_returnsTheMovedEnvelope() throws Exception {
        writer.receive(capturedEnvelope());

        Collection<Envelope> moved = updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        assertThat(moved).extracting(Envelope::status).containsExactly(EnvelopeStatus.PERSISTED);
    }

    @Test
    void update_thenRead_hasAnUpdatedAtFromTheMoveAndNotFromTheCapture() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        Collection<Envelope> persisted = reader.findByStatus(EnvelopeStatus.PERSISTED);
        assertThat(persisted).hasSize(1);
        Envelope read = persisted.iterator().next();
        assertThat(read.status()).isEqualTo(EnvelopeStatus.PERSISTED);
        assertThat(read.updatedAt()).isAfter(Instant.now().minus(Duration.ofMinutes(1)));
    }

    @Test
    void update_toTheSameStatus_keepsTheObject() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.CAPTURED);

        assertThat(s3Client.keys()).containsExactly("inbox/CAPTURED/" + KEY);
    }

    private byte[] stored(String key) {
        return s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).asByteArray();
    }

    private static Envelope capturedEnvelope() {
        return new Envelope(
                IdempotencyKey.of(KEY),
                new EventMetadata(),
                MediaTypes.JSON,
                "{}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                0,
                CAPTURED_AT,
                null);
    }
}

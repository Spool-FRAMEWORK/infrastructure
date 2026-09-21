package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class S3InboxUpdatedAtTest {

    private static final String BUCKET = "spool-inbox-test";
    private static final String KEY = "republished-key";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);
    private final S3InboxUpdater updater = new S3InboxUpdater(s3Client, BUCKET);
    private final S3InboxReader reader = new S3InboxReader(s3Client, BUCKET);
    private final S3InboxRemover remover = new S3InboxRemover(s3Client, BUCKET);

    @Test
    void findByStatus_afterARepublication_readsTheEnvelopeWithItsUpdatedAt() throws Exception {
        Envelope republished = republish();

        Collection<Envelope> found = reader.findByStatus(EnvelopeStatus.CAPTURED);

        assertThat(found).hasSize(1);
        Envelope read = found.iterator().next();
        assertThat(read.retries()).isEqualTo(1);
        assertThat(read.updatedAt()).isCloseTo(republished.updatedAt(), within(1, ChronoUnit.MILLIS));
    }

    @Test
    void findById_afterARepublication_readsTheEnvelope() throws Exception {
        Envelope republished = republish();

        Optional<Envelope> found = reader.findById(IdempotencyKey.of(KEY));

        assertThat(found).isPresent();
        assertThat(found.get().retries()).isEqualTo(1);
        assertThat(found.get().updatedAt()).isCloseTo(republished.updatedAt(), within(1, ChronoUnit.MILLIS));
    }

    @Test
    void remove_aRepublishedEnvelope_returnsItAndDeletesTheObject() throws Exception {
        republish();

        Collection<Envelope> removed = remover.remove(List.of(IdempotencyKey.of(KEY)));

        assertThat(removed).hasSize(1);
        assertThat(s3Client.keys()).isEmpty();
    }

    @Test
    void findByStatus_anObjectWithUnknownFields_isStillRead() throws Exception {
        writer.receive(capturedEnvelope());
        String objectKey = "inbox/CAPTURED/" + KEY;
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) mapper.readTree(stored(objectKey));
        node.put("unexpected", "value");
        store(objectKey, mapper.writeValueAsBytes(node));

        assertThat(reader.findByStatus(EnvelopeStatus.CAPTURED)).hasSize(1);
    }

    @Test
    void findByStatus_anObjectWhoseJsonHasAStaleStatus_returnsTheFolderStatus() throws Exception {
        moveWithoutRewriting(EnvelopeStatus.PERSISTED);

        Collection<Envelope> found = reader.findByStatus(EnvelopeStatus.PERSISTED);

        assertThat(found).extracting(Envelope::status).containsExactly(EnvelopeStatus.PERSISTED);
    }

    @Test
    void findById_anObjectWhoseJsonHasAStaleStatus_returnsTheFolderStatus() throws Exception {
        moveWithoutRewriting(EnvelopeStatus.QUARANTINED);

        Optional<Envelope> found = reader.findById(IdempotencyKey.of(KEY));

        assertThat(found).isPresent();
        assertThat(found.get().status()).isEqualTo(EnvelopeStatus.QUARANTINED);
    }

    private Envelope republish() throws Exception {
        Envelope envelope = capturedEnvelope();
        writer.receive(envelope);
        Envelope republished = envelope.retry();
        updater.update(republished);
        return republished;
    }

    private void moveWithoutRewriting(EnvelopeStatus status) throws Exception {
        writer.receive(capturedEnvelope());
        String sourceKey = "inbox/CAPTURED/" + KEY;
        byte[] content = stored(sourceKey);
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(sourceKey).build());
        store("inbox/" + status.name() + "/" + KEY, content);
    }

    private byte[] stored(String key) {
        return s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).asByteArray();
    }

    private void store(String key, byte[] content) {
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(content));
    }

    private static Envelope capturedEnvelope() {
        return new Envelope(
                IdempotencyKey.of(KEY),
                new EventMetadata(),
                MediaTypes.JSON,
                "{}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                0,
                Instant.now().minus(Duration.ofMinutes(10)),
                null);
    }
}

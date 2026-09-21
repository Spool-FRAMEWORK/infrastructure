package software.spool.infrastructure.adapter.inbox.filesystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemInboxUpdaterMoveTest {

    private static final String KEY = "moved-key";
    private static final Instant CAPTURED_AT = Instant.now().minus(Duration.ofDays(2));

    @TempDir
    Path directory;

    private FileSystemInboxWriter writer;
    private FileSystemInboxUpdater updater;
    private FileSystemInboxReader reader;

    @BeforeEach
    void createAdapters() {
        writer = new FileSystemInboxWriter(directory.toString());
        updater = new FileSystemInboxUpdater(directory.toString());
        reader = new FileSystemInboxReader(directory.toString());
    }

    @Test
    void update_toPersisted_movesTheFile() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        assertThat(Files.exists(directory.resolve("PERSISTED").resolve(KEY + ".json"))).isTrue();
        assertThat(Files.exists(directory.resolve("CAPTURED").resolve(KEY + ".json"))).isFalse();
    }

    @Test
    void update_toPersisted_rewritesTheStoredStatusAndUpdatedAt() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        Optional<Envelope> found = reader.findById(IdempotencyKey.of(KEY));
        assertThat(found).isPresent();
        assertThat(found.get().status()).isEqualTo(EnvelopeStatus.PERSISTED);
        assertThat(found.get().updatedAt()).isAfter(Instant.now().minus(Duration.ofMinutes(1)));
        assertThat(found.get().capturedAt()).isBefore(Instant.now().minus(Duration.ofDays(1)));
    }

    @Test
    void update_toPersisted_returnsTheMovedEnvelope() throws Exception {
        writer.receive(capturedEnvelope());

        Collection<Envelope> moved = updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        assertThat(moved).hasSize(1);
        Envelope envelope = moved.iterator().next();
        assertThat(envelope.status()).isEqualTo(EnvelopeStatus.PERSISTED);
        assertThat(envelope.updatedAt()).isNotNull();
    }

    @Test
    void update_toTheSameStatus_keepsTheFile() throws Exception {
        writer.receive(capturedEnvelope());

        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.CAPTURED);

        assertThat(Files.exists(directory.resolve("CAPTURED").resolve(KEY + ".json"))).isTrue();
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

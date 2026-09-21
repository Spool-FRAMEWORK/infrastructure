package software.spool.infrastructure.adapter.inbox.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3InboxWriterDuplicateTest {

    private static final String BUCKET = "spool-inbox-test";
    private static final String KEY = "duplicate-key";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);

    @Test
    void receive_newKey_writesTheCapturedObject() throws Exception {
        IdempotencyKey received = writer.receive(capturedEnvelope());

        assertThat(received.value()).isEqualTo(KEY);
        assertThat(s3Client.keys()).containsExactly("inbox/CAPTURED/" + KEY);
    }

    @ParameterizedTest
    @EnumSource(EnvelopeStatus.class)
    void receive_keyAlreadyInAnyStatus_throwsDuplicateEventException(EnvelopeStatus existingStatus) {
        s3Client.put("inbox/" + existingStatus.name() + "/" + KEY);

        assertThatThrownBy(() -> writer.receive(capturedEnvelope()))
                .isInstanceOf(DuplicateEventException.class);
    }

    @ParameterizedTest
    @EnumSource(EnvelopeStatus.class)
    void receive_duplicateInAnyStatus_writesNothing(EnvelopeStatus existingStatus) {
        String existingKey = "inbox/" + existingStatus.name() + "/" + KEY;
        s3Client.put(existingKey);

        assertThatThrownBy(() -> writer.receive(capturedEnvelope()))
                .isInstanceOf(DuplicateEventException.class);
        assertThat(s3Client.keys()).containsExactly(existingKey);
    }

    @Test
    void receive_afterTheUpdaterMovedTheEnvelope_throwsDuplicateEventException() throws Exception {
        writer.receive(capturedEnvelope());
        new S3InboxUpdater(s3Client, BUCKET).update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);

        assertThatThrownBy(() -> writer.receive(capturedEnvelope()))
                .isInstanceOf(DuplicateEventException.class);
        assertThat(s3Client.keys()).containsExactly("inbox/PERSISTED/" + KEY);
    }

    private static Envelope capturedEnvelope() {
        return new Envelope(
                IdempotencyKey.of(KEY),
                new EventMetadata(),
                MediaTypes.JSON,
                "{}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                0,
                Instant.now(),
                null);
    }
}

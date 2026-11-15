package software.spool.infrastructure.adapter.inbox;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rules every {@link InboxUpdater} adapter must follow when it moves envelopes to another status or writes one.
 */
public abstract class InboxUpdaterContract {

    private static final IdempotencyKey FIRST_KEY = IdempotencyKey.of("first-key");
    private static final IdempotencyKey MISSING_KEY = IdempotencyKey.of("missing-key");
    private static final IdempotencyKey LAST_KEY = IdempotencyKey.of("last-key");

    protected abstract InboxUpdater updater();

    protected abstract void store(Envelope envelope) throws Exception;

    protected abstract Optional<Envelope> find(IdempotencyKey idempotencyKey) throws Exception;

    @Test
    void update_keyNotInTheInboxBetweenTwoThatAre_skipsItAndMovesTheOthers() throws Exception {
        store(capturedEnvelope(FIRST_KEY));
        store(capturedEnvelope(LAST_KEY));

        Collection<Envelope> moved = updater().update(List.of(FIRST_KEY, MISSING_KEY, LAST_KEY), EnvelopeStatus.PERSISTED);

        assertThat(moved).extracting(Envelope::idempotencyKey).containsExactly(FIRST_KEY, LAST_KEY);
        assertThat(find(FIRST_KEY)).map(Envelope::status).contains(EnvelopeStatus.PERSISTED);
        assertThat(find(LAST_KEY)).map(Envelope::status).contains(EnvelopeStatus.PERSISTED);
        assertThat(find(MISSING_KEY)).isEmpty();
    }

    @Test
    void update_onlyKeysNotInTheInbox_movesNothing() throws Exception {
        Collection<Envelope> moved = updater().update(List.of(MISSING_KEY), EnvelopeStatus.PERSISTED);

        assertThat(moved).isEmpty();
        assertThat(find(MISSING_KEY)).isEmpty();
    }

    @Test
    void updateEnvelope_keyNotInTheInbox_writesItUnderItsStatus() throws Exception {
        Envelope envelope = capturedEnvelope(MISSING_KEY);

        updater().update(envelope);

        assertThat(find(MISSING_KEY)).hasValueSatisfying(written -> {
            assertThat(written.status()).isEqualTo(EnvelopeStatus.CAPTURED);
            assertThat(written.payload()).isEqualTo(envelope.payload());
        });
    }

    private static Envelope capturedEnvelope(IdempotencyKey idempotencyKey) {
        Instant capturedAt = Instant.now();
        return new Envelope(
                idempotencyKey,
                new EventMetadata(),
                MediaTypes.JSON,
                "{}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                0,
                capturedAt,
                capturedAt);
    }
}

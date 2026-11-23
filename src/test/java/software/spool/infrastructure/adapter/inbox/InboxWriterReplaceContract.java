package software.spool.infrastructure.adapter.inbox;

import org.junit.jupiter.api.Test;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxReader;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.utils.media.MediaTypes;
import software.spool.crawler.api.port.InboxWriter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How every inbox treats a capture whose key is in quarantine (ADR-032): refused when it keeps what is in quarantine,
 * and taking its place when it replaces it with another payload.
 */
public abstract class InboxWriterReplaceContract {
    private static final IdempotencyKey KEY = IdempotencyKey.of("boe-a-1");

    protected abstract InboxWriter writer();

    protected abstract InboxUpdater updater();

    protected abstract InboxReader reader();

    @Test
    void keeping_aKeyInQuarantine_isADuplicate() {
        inQuarantine("{\"amount\": -5}");

        assertThatThrownBy(() -> writer().receive(envelope("{\"amount\": 5}"))).isInstanceOf(DuplicateEventException.class);
        assertThat(reader().findById(KEY)).hasValueSatisfying(envelope ->
                assertThat(envelope.status()).isEqualTo(EnvelopeStatus.QUARANTINED));
    }

    @Test
    void replacing_aKeyInQuarantineWithAnotherPayload_storesTheNewEnvelopeAsCapturedInItsPlace() {
        inQuarantine("{\"amount\": -5}");

        InboxWriter.Receipt receipt = writer().receiveReplacingQuarantined(envelope("{\"amount\": 5}"));

        assertThat(receipt).isEqualTo(InboxWriter.Receipt.replacedQuarantined(KEY));
        assertThat(reader().findByStatus(EnvelopeStatus.QUARANTINED)).isEmpty();
        assertThat(reader().findByStatus(EnvelopeStatus.CAPTURED)).singleElement().satisfies(envelope ->
                assertThat(new String(envelope.payload(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\": 5}"));
    }

    @Test
    void replacing_aKeyInQuarantineWithTheSamePayload_isADuplicate() {
        inQuarantine("{\"amount\": -5}");

        assertThatThrownBy(() -> writer().receiveReplacingQuarantined(envelope("{\"amount\": -5}")))
                .isInstanceOf(DuplicateEventException.class);
        assertThat(reader().findByStatus(EnvelopeStatus.QUARANTINED)).hasSize(1);
    }

    @Test
    void replacing_aKeyThatIsNotInQuarantine_isADuplicate() {
        writer().receive(envelope("{\"amount\": 5}"));
        updater().update(List.of(KEY), EnvelopeStatus.PERSISTED);

        assertThatThrownBy(() -> writer().receiveReplacingQuarantined(envelope("{\"amount\": 7}")))
                .isInstanceOf(DuplicateEventException.class);
    }

    @Test
    void replacing_aNewKey_storesItAsReceiveDoes() {
        assertThat(writer().receiveReplacingQuarantined(envelope("{\"amount\": 5}"))).isEqualTo(InboxWriter.Receipt.stored(KEY));
        assertThat(reader().findByStatus(EnvelopeStatus.CAPTURED)).hasSize(1);
    }

    private void inQuarantine(String payload) {
        writer().receive(envelope(payload));
        updater().update(List.of(KEY), EnvelopeStatus.QUARANTINED);
    }

    private static Envelope envelope(String payload) {
        return new Envelope(KEY, new EventMetadata(), MediaTypes.JSON, payload.getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED, 0, Instant.now(), null);
    }
}

package software.spool.infrastructure.adapter.quarantine;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rules every {@link QuarantineRepository} adapter must follow.
 */
public abstract class QuarantineRepositoryContract {

    private static final IdempotencyKey FIRST_KEY = IdempotencyKey.of("first-key");
    private static final IdempotencyKey SECOND_KEY = IdempotencyKey.of("second-key");
    private static final Instant QUARANTINED_AT = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    protected abstract QuarantineRepository repository();

    @Test
    void find_savedRecord_returnsTheEnvelopeReasonViolationsAndDate() {
        repository().save(validationRecord(FIRST_KEY));

        Optional<QuarantinedRecord> found = repository().find(FIRST_KEY);

        assertThat(found).isPresent();
        QuarantinedRecord quarantinedRecord = found.get();
        assertThat(quarantinedRecord.idempotencyKey()).isEqualTo(FIRST_KEY);
        assertThat(quarantinedRecord.envelope().payload()).isEqualTo("{\"price\":-1}".getBytes(StandardCharsets.UTF_8));
        assertThat(quarantinedRecord.envelope().status()).isEqualTo(EnvelopeStatus.CAPTURED);
        assertThat(quarantinedRecord.envelope().metadata().get(EventMetadataKey.SOURCE)).isEqualTo("boe");
        assertThat(quarantinedRecord.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
        assertThat(quarantinedRecord.violations()).containsExactly("price must be positive");
        assertThat(quarantinedRecord.quarantinedAt()).isEqualTo(QUARANTINED_AT);
    }

    @Test
    void find_keyWithoutRecord_returnsEmpty() {
        assertThat(repository().find(FIRST_KEY)).isEmpty();
    }

    @Test
    void save_sameKeyTwice_keepsOnlyTheLastRecord() {
        repository().save(retriesRecord(FIRST_KEY));
        repository().save(validationRecord(FIRST_KEY));

        assertThat(repository().findAll()).hasSize(1);
        assertThat(repository().find(FIRST_KEY)).map(QuarantinedRecord::reason).contains(QuarantineReason.VALIDATION_FAILED);
    }

    @Test
    void findAll_twoSavedRecords_returnsBoth() {
        repository().save(validationRecord(FIRST_KEY));
        repository().save(retriesRecord(SECOND_KEY));

        assertThat(repository().findAll()).extracting(QuarantinedRecord::idempotencyKey)
                .containsExactlyInAnyOrder(FIRST_KEY, SECOND_KEY);
    }

    @Test
    void delete_savedRecord_removesOnlyThatOne() {
        repository().save(validationRecord(FIRST_KEY));
        repository().save(retriesRecord(SECOND_KEY));

        repository().delete(FIRST_KEY);

        assertThat(repository().find(FIRST_KEY)).isEmpty();
        assertThat(repository().find(SECOND_KEY)).isPresent();
    }

    @Test
    void delete_keyWithoutRecord_doesNothing() {
        repository().delete(FIRST_KEY);

        assertThat(repository().findAll()).isEmpty();
    }

    private static QuarantinedRecord validationRecord(IdempotencyKey idempotencyKey) {
        return new QuarantinedRecord(envelope(idempotencyKey), QuarantineReason.VALIDATION_FAILED,
                List.of("price must be positive"), QUARANTINED_AT);
    }

    private static QuarantinedRecord retriesRecord(IdempotencyKey idempotencyKey) {
        return new QuarantinedRecord(envelope(idempotencyKey), QuarantineReason.RETRIES_EXHAUSTED, List.of(), QUARANTINED_AT);
    }

    private static Envelope envelope(IdempotencyKey idempotencyKey) {
        return new Envelope(
                idempotencyKey,
                new EventMetadata().set(EventMetadataKey.SOURCE, "boe"),
                MediaTypes.JSON,
                "{\"price\":-1}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                3,
                QUARANTINED_AT,
                QUARANTINED_AT);
    }
}

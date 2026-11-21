package software.spool.infrastructure.adapter.dataLake;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.core.port.datalake.BronzeWriter;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Rules every bronze adapter must follow: its {@link BronzeWriter} and its {@link BronzeEnvelopeRemover} work on the
 * same envelopes.
 */
public abstract class BronzeContract {

    protected static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("year", "2026")
            .with("source", "boe").build();
    protected static final PartitionKey OTHER_PARTITION = PartitionKey.ofEntriesWithoutDate().with("year", "2025")
            .with("source", "boe").build();
    private static final IdempotencyKey KEY = IdempotencyKey.of("k1");
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    protected abstract BronzeWriter writer();

    protected abstract BronzeEnvelopeRemover remover();

    protected abstract Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) throws Exception;

    @Test
    void save_envelope_isFoundInItsPartitionWithItsContent() throws Exception {
        writer().save(PARTITION, envelope("{\"price\":10}"));

        assertThat(find(PARTITION, KEY)).hasValueSatisfying(found -> {
            assertThat(found.payload()).isEqualTo("{\"price\":10}".getBytes(StandardCharsets.UTF_8));
            assertThat(found.metadata().get(EventMetadataKey.SOURCE)).isEqualTo("boe");
        });
        assertThat(find(OTHER_PARTITION, KEY)).isEmpty();
    }

    @Test
    void save_sameKeyTwice_keepsTheLastContent() throws Exception {
        writer().save(PARTITION, envelope("{\"price\":10}"));
        writer().save(PARTITION, envelope("{\"price\":20}"));

        assertThat(find(PARTITION, KEY)).map(Envelope::payload).contains("{\"price\":20}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void remove_savedEnvelope_removesItOnlyFromThatPartition() throws Exception {
        writer().save(PARTITION, envelope("{\"price\":10}"));
        writer().save(OTHER_PARTITION, envelope("{\"price\":10}"));

        remover().remove(PARTITION, KEY);

        assertThat(find(PARTITION, KEY)).isEmpty();
        assertThat(find(OTHER_PARTITION, KEY)).isPresent();
    }

    @Test
    void remove_envelopeThatIsNotThere_doesNothing() {
        assertThatCode(() -> remover().remove(PARTITION, KEY)).doesNotThrowAnyException();
    }

    private static Envelope envelope(String payload) {
        return new Envelope(KEY, new EventMetadata().set(EventMetadataKey.SOURCE, "boe"), MediaTypes.JSON,
                payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.PERSISTED, 0, NOW, NOW);
    }
}

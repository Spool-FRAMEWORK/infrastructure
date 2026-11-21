package software.spool.infrastructure.adapter.dataLake;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Rules every data lake adapter must follow: its {@link DataLakeWriter} and its {@link DataLakeEnvelopeRemover} work
 * on the same envelopes.
 */
public abstract class DataLakeContract {

    protected static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("year", "2026")
            .with("source", "boe").build();
    protected static final PartitionKey OTHER_PARTITION = PartitionKey.ofEntriesWithoutDate().with("year", "2025")
            .with("source", "boe").build();
    private static final IdempotencyKey KEY = IdempotencyKey.of("k1");
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    protected abstract DataLakeWriter writer();

    protected abstract DataLakeEnvelopeRemover remover();

    protected abstract Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) throws Exception;

    @Test
    void write_envelope_returnsItsKeyAndIsFoundInItsPartitionWithItsContent() throws Exception {
        assertThat(write(PARTITION, "{\"price\":10}")).containsExactly(KEY);

        assertThat(find(PARTITION, KEY)).hasValueSatisfying(found -> {
            assertThat(found.payload()).isEqualTo("{\"price\":10}".getBytes(StandardCharsets.UTF_8));
            assertThat(found.metadata().get(EventMetadataKey.SOURCE)).isEqualTo("boe");
        });
        assertThat(find(OTHER_PARTITION, KEY)).isEmpty();
    }

    @Test
    void write_sameKeyTwice_keepsTheLastContent() throws Exception {
        write(PARTITION, "{\"price\":10}");
        write(PARTITION, "{\"price\":20}");

        assertThat(find(PARTITION, KEY)).map(Envelope::payload).contains("{\"price\":20}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void remove_writtenEnvelope_removesItOnlyFromThatPartition() throws Exception {
        write(PARTITION, "{\"price\":10}");
        write(OTHER_PARTITION, "{\"price\":10}");

        remover().remove(PARTITION, KEY);

        assertThat(find(PARTITION, KEY)).isEmpty();
        assertThat(find(OTHER_PARTITION, KEY)).isPresent();
    }

    @Test
    void remove_envelopeThatIsNotThere_doesNothing() {
        assertThatCode(() -> remover().remove(PARTITION, KEY)).doesNotThrowAnyException();
    }

    private List<IdempotencyKey> write(PartitionKey partition, String payload) {
        return writer().write(List.of(new PartitionedEnvelope(partition, envelope(payload)))).toList();
    }

    private static Envelope envelope(String payload) {
        return new Envelope(KEY, new EventMetadata().set(EventMetadataKey.SOURCE, "boe"), MediaTypes.JSON,
                payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.PERSISTED, 0, NOW, NOW);
    }
}

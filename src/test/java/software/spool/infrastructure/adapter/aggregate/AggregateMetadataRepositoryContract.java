package software.spool.infrastructure.adapter.aggregate;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.ConsumedPartition;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

abstract class AggregateMetadataRepositoryContract {
    private static final String GOLD = "gold/payments-by-country";
    private static final PartitionKey SPAIN = new PartitionKey("country=ES");
    private static final PartitionKey FRANCE = new PartitionKey("country=FR");
    private static final PartitionKey TEN = new PartitionKey("payments::year=2026::month=11::day=17::hour=10");
    private static final PartitionKey ELEVEN = new PartitionKey("payments::year=2026::month=11::day=17::hour=11");

    abstract AggregateMetadataRepository repository();

    /** Writes the content at that key of the piece, as an older version of Spool would have left it. */
    abstract void keepAsIs(String key, byte[] content);

    @Test
    void find_savedAggregate_givesBackEverythingItKeeps() {
        AggregateMetadata saved = new AggregateMetadata(GOLD, SPAIN, Set.of(TEN, ELEVEN), 2,
                Instant.parse("2026-11-17T12:00:00Z"), "{\"sum\":40,\"count\":2}", Optional.of("{\"sum\":10,\"count\":1}"));

        repository().save(saved);

        assertThat(repository().find(GOLD, SPAIN)).contains(saved);
    }

    @Test
    void find_aggregateNeverPublished_hasNoPublishedState() {
        AggregateMetadata saved = metadata(SPAIN, "{\"sum\":10}");

        repository().save(saved);

        assertThat(repository().find(GOLD, SPAIN)).hasValueSatisfying(found -> assertThat(found.publishedState()).isEmpty());
    }

    @Test
    void find_aggregateNotSaved_isEmpty() {
        assertThat(repository().find(GOLD, SPAIN)).isEmpty();
    }

    @Test
    void save_sameAggregateAgain_replacesIt() {
        repository().save(metadata(SPAIN, "{\"sum\":10}"));
        repository().save(metadata(SPAIN, "{\"sum\":15}"));

        assertThat(repository().findAll(GOLD)).singleElement()
                .satisfies(found -> assertThat(found.state()).isEqualTo("{\"sum\":15}"));
    }

    @Test
    void findAll_givesTheAggregatesOfThatDataMartOnly() {
        repository().save(metadata(SPAIN, "{\"sum\":10}"));
        repository().save(metadata(FRANCE, "{\"sum\":5}"));
        repository().save(new AggregateMetadata("gold/refunds", SPAIN, Set.of(TEN), 1, Instant.EPOCH, "{}", Optional.empty()));

        assertThat(repository().findAll(GOLD)).extracting(AggregateMetadata::aggregate).containsExactlyInAnyOrder(SPAIN, FRANCE);
    }

    @Test
    void consumedSources_addsUpEveryPartitionMarkedWithItsFiles() {
        repository().markConsumed(GOLD, Set.of(ConsumedPartition.of(TEN, OptionalLong.of(2))));
        repository().markConsumed(GOLD, Set.of(ConsumedPartition.withoutFiles(ELEVEN)));

        assertThat(repository().consumedSources(GOLD)).containsExactlyInAnyOrder(
                ConsumedPartition.of(TEN, OptionalLong.of(2)), ConsumedPartition.withoutFiles(ELEVEN));
        assertThat(repository().consumedSources("gold/refunds")).isEmpty();
    }

    @Test
    void markConsumed_partitionMarkedAgain_keepsOnlyItsLastFiles() {
        repository().markConsumed(GOLD, Set.of(ConsumedPartition.of(TEN, OptionalLong.of(2))));

        repository().markConsumed(GOLD, Set.of(ConsumedPartition.of(TEN, OptionalLong.of(3))));

        assertThat(repository().consumedSources(GOLD)).containsExactly(ConsumedPartition.of(TEN, OptionalLong.of(3)));
    }

    @Test
    void consumedSources_keptBeforeTheirFilesWere_areGivenBackWithoutFiles() {
        keepAsIs(AggregateMetadataLayout.consumedSourcesKeyOf(GOLD),
                ("[\"" + TEN.value() + "\",\"" + ELEVEN.value() + "\"]").getBytes(StandardCharsets.UTF_8));

        assertThat(repository().consumedSources(GOLD)).containsExactlyInAnyOrder(
                ConsumedPartition.withoutFiles(TEN), ConsumedPartition.withoutFiles(ELEVEN));
    }

    private static AggregateMetadata metadata(PartitionKey aggregate, String state) {
        return new AggregateMetadata(GOLD, aggregate, Set.of(TEN), 1, Instant.parse("2026-11-17T12:00:00Z"), state, Optional.empty());
    }
}

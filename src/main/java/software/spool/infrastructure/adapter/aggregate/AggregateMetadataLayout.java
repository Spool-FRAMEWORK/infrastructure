package software.spool.infrastructure.adapter.aggregate;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;

/**
 * Where the metadata of the aggregates is kept, relative to the root of the piece, with {@code /} between folders:
 * {@code <dataMart>/aggregates/<hash of the aggregate>.json} for each aggregate, and
 * {@code <dataMart>/consumed-sources.json} for the partitions of silver the data mart has consumed. The data mart is
 * named with its layer, as in {@code gold/payments}.
 */
final class AggregateMetadataLayout {
    private static final String SEPARATOR = "/";
    private static final String AGGREGATES = "aggregates";
    private static final String CONSUMED_SOURCES = "consumed-sources.json";
    private static final String EXTENSION = ".json";
    private static final String AGGREGATE_NAMESPACE = "aggregate";

    private AggregateMetadataLayout() {
    }

    static String aggregatesFolderOf(String dataMart) {
        return dataMart + SEPARATOR + AGGREGATES;
    }

    static String aggregateKeyOf(String dataMart, PartitionKey aggregate) {
        return aggregatesFolderOf(dataMart) + SEPARATOR + IdempotencyKey.of(AGGREGATE_NAMESPACE, aggregate.value()).value() + EXTENSION;
    }

    static String consumedSourcesKeyOf(String dataMart) {
        return dataMart + SEPARATOR + CONSUMED_SOURCES;
    }

    static boolean isAggregateKey(String key) {
        return key.endsWith(EXTENSION);
    }
}

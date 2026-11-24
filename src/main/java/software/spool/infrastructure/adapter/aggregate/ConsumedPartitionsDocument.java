package software.spool.infrastructure.adapter.aggregate;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.model.ConsumedPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The partitions of silver a data mart has consumed, as they are kept in a JSON document: a list of
 * {@code {"partition": ..., "files": ...}}, without {@code files} when the number is unknown (ADR-037). A list of
 * partition names, as it was kept before, is read as partitions without their number of files.
 */
final class ConsumedPartitionsDocument {
    private static final String PARTITION = "partition";
    private static final String FILES = "files";
    private static final RecordSerializer<Object> SERIALIZER = RecordSerializerFactory.record();
    private static final PayloadDeserializer<Object[]> ENTRIES = PayloadDeserializerFactory.json().as(Object[].class);

    private ConsumedPartitionsDocument() {
    }

    static byte[] serialize(Collection<ConsumedPartition> consumed) {
        return SERIALIZER.serialize(consumed.stream()
                .sorted(Comparator.comparing(partition -> partition.partition().value()))
                .map(ConsumedPartitionsDocument::entryOf)
                .toList());
    }

    static Set<ConsumedPartition> deserialize(byte[] content) {
        return Arrays.stream(ENTRIES.deserialize(content))
                .map(ConsumedPartitionsDocument::consumedOf)
                .collect(Collectors.toSet());
    }

    /** The partitions kept, with the ones marked in place of the kept ones of the same partition. */
    static Set<ConsumedPartition> merged(Set<ConsumedPartition> kept, Set<ConsumedPartition> marked) {
        Map<PartitionKey, ConsumedPartition> byPartition = new HashMap<>();
        kept.forEach(partition -> byPartition.put(partition.partition(), partition));
        marked.forEach(partition -> byPartition.put(partition.partition(), partition));
        return Set.copyOf(byPartition.values());
    }

    private static Map<String, Object> entryOf(ConsumedPartition consumed) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put(PARTITION, consumed.partition().value());
        consumed.files().ifPresent(files -> entry.put(FILES, files));
        return entry;
    }

    private static ConsumedPartition consumedOf(Object entry) {
        if (entry instanceof String partition) return ConsumedPartition.withoutFiles(new PartitionKey(partition));
        if (entry instanceof Map<?, ?> fields && fields.get(PARTITION) instanceof String partition) {
            OptionalLong files = fields.get(FILES) instanceof Number number ? OptionalLong.of(number.longValue()) : OptionalLong.empty();
            return ConsumedPartition.of(new PartitionKey(partition), files);
        }
        throw new IllegalStateException("A consumed partition is kept as " + entry
                + ", which is neither a partition name nor an object with a partition");
    }
}

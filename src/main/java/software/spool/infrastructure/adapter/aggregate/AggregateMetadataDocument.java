package software.spool.infrastructure.adapter.aggregate;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.model.AggregateMetadata;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The metadata of an aggregate as it is kept in a JSON document. A {@code publishedState} of {@code null} means nothing
 * has been published yet.
 */
record AggregateMetadataDocument(String dataMart, String aggregate, List<String> sources, long records,
                                 Instant updatedAt, String state, String publishedState) {
    private static final RecordSerializer<Object> SERIALIZER = RecordSerializerFactory.record();
    private static final PayloadDeserializer<AggregateMetadataDocument> DESERIALIZER =
            PayloadDeserializerFactory.json().as(AggregateMetadataDocument.class);
    private static final PayloadDeserializer<String[]> SOURCES = PayloadDeserializerFactory.json().as(String[].class);

    static byte[] serialize(AggregateMetadata metadata) {
        return SERIALIZER.serialize(new AggregateMetadataDocument(metadata.dataMart(), metadata.aggregate().value(),
                metadata.sources().stream().map(PartitionKey::value).sorted().toList(), metadata.records(),
                metadata.updatedAt(), metadata.state(), metadata.publishedState().orElse(null)));
    }

    static AggregateMetadata deserialize(byte[] content) {
        AggregateMetadataDocument document = DESERIALIZER.deserialize(content);
        return new AggregateMetadata(document.dataMart(), new PartitionKey(document.aggregate()),
                document.sources().stream().map(PartitionKey::new).collect(Collectors.toSet()), document.records(),
                document.updatedAt(), document.state(), Optional.ofNullable(document.publishedState()));
    }

    static byte[] serializeSources(Set<PartitionKey> sources) {
        return SERIALIZER.serialize(sources.stream().map(PartitionKey::value).sorted().toList());
    }

    static Set<PartitionKey> deserializeSources(byte[] content) {
        return java.util.Arrays.stream(SOURCES.deserialize(content)).map(PartitionKey::new).collect(Collectors.toSet());
    }
}

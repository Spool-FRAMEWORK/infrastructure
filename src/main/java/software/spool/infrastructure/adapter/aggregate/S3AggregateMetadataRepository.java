package software.spool.infrastructure.adapter.aggregate;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.S3ObjectKeys;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Keeps the metadata of the aggregates in a bucket, one JSON object each, laid out by {@link AggregateMetadataLayout}. */
public class S3AggregateMetadataRepository implements AggregateMetadataRepository {
    private final S3Client s3Client;
    private final String bucket;

    public S3AggregateMetadataRepository(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public Optional<AggregateMetadata> find(String dataMart, PartitionKey aggregate) {
        return get(AggregateMetadataLayout.aggregateKeyOf(dataMart, aggregate)).map(AggregateMetadataDocument::deserialize);
    }

    @Override
    public List<AggregateMetadata> findAll(String dataMart) {
        return S3ObjectKeys.under(s3Client, bucket, AggregateMetadataLayout.aggregatesFolderOf(dataMart) + "/").stream()
                .filter(AggregateMetadataLayout::isAggregateKey)
                .sorted()
                .map(key -> get(key).map(AggregateMetadataDocument::deserialize))
                .flatMap(Optional::stream)
                .toList();
    }

    @Override
    public void save(AggregateMetadata metadata) {
        put(AggregateMetadataLayout.aggregateKeyOf(metadata.dataMart(), metadata.aggregate()),
                AggregateMetadataDocument.serialize(metadata));
    }

    @Override
    public Set<PartitionKey> consumedSources(String dataMart) {
        return get(AggregateMetadataLayout.consumedSourcesKeyOf(dataMart))
                .map(AggregateMetadataDocument::deserializeSources)
                .orElse(Set.of());
    }

    @Override
    public void markConsumed(String dataMart, Set<PartitionKey> sources) {
        if (sources.isEmpty()) return;
        Set<PartitionKey> consumed = new HashSet<>(consumedSources(dataMart));
        consumed.addAll(sources);
        put(AggregateMetadataLayout.consumedSourcesKeyOf(dataMart), AggregateMetadataDocument.serializeSources(consumed));
    }

    private Optional<byte[]> get(String key) {
        try {
            return Optional.of(s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        }
    }

    private void put(String key, byte[] content) {
        s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).contentLength((long) content.length).build(),
                RequestBody.fromBytes(content));
    }
}

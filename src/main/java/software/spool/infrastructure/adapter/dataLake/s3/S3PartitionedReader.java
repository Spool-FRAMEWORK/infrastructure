package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.DataLakeReadException;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.infrastructure.adapter.dataLake.EnvelopeRecords;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.scaling.PartitionSlice;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Reads a partition of the data lake, and the partitions below it, from a bucket laid out by {@link S3DataLakeLayout},
 * several objects at a time on the threads of a {@link BoundedConcurrency}: the envelopes of bronze for a
 * {@link MountMode#TRANSFORMATION} mount, and the records of silver for a {@link MountMode#AGGREGATION} one.
 *
 * <p>The leading entries of the partition are followed as a prefix while the bucket has objects under them. From the
 * first one it has not, such as {@code source=payments} kept behind the date entries, the objects below the last
 * prefix are read when their folder has every remaining entry, wherever it is in their path.</p>
 *
 * <p>The header of each envelope comes from the metadata of its payload object or, when it did not fit there, from
 * the object next to it. A payload without a header is an envelope still being written, and is skipped.</p>
 */
public class S3PartitionedReader implements PartitionedReader {
    private static final Logger LOG = LoggerFactory.getLogger(S3PartitionedReader.class);
    private static final PayloadDeserializer<EnvelopeHeader> HEADERS = PayloadDeserializerFactory.json().as(EnvelopeHeader.class);

    private final S3Client s3Client;
    private final String bucket;
    private final BoundedConcurrency concurrency;

    public S3PartitionedReader(S3Client s3Client, String bucket, BoundedConcurrency concurrency) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        String layer = S3DataLakeLayout.layerReadBy(mountTarget.mode());
        List<String> objectKeys = objectKeysOf(layer, mountTarget.sourceKey());
        Set<String> existingKeys = new HashSet<>(objectKeys);
        List<String> recordKeys = objectKeys.stream().filter(key -> !S3DataLakeLayout.isHeaderKey(key)).toList();
        return concurrency.map(sliceOf(recordKeys, mountTarget.slice()),
                        key -> recordAt(layer, key, existingKeys, mountTarget.sourceKey()))
                .flatMap(Optional::stream);
    }

    private List<String> objectKeysOf(String layer, PartitionKey partition) {
        List<String> entries = KeyEntries.split(partition.value());
        int leadingEntries = 0;
        while (leadingEntries < entries.size()
                && hasObjectsUnder(S3DataLakeLayout.prefixOf(layer, entries.subList(0, leadingEntries + 1)))) {
            leadingEntries++;
        }
        List<String> remainingEntries = entries.subList(leadingEntries, entries.size());
        return objectKeysUnder(S3DataLakeLayout.prefixOf(layer, entries.subList(0, leadingEntries))).stream()
                .filter(key -> S3DataLakeLayout.isInAFolderWithEveryEntry(layer, key, remainingEntries))
                .toList();
    }

    private boolean hasObjectsUnder(String prefix) {
        return !s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1).build())
                .contents().isEmpty();
    }

    private List<String> objectKeysUnder(String prefix) {
        List<String> keys = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Response listing = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .continuationToken(continuationToken)
                    .build());
            listing.contents().stream().map(S3Object::key).forEach(keys::add);
            continuationToken = Boolean.TRUE.equals(listing.isTruncated()) ? listing.nextContinuationToken() : null;
        } while (continuationToken != null);
        return keys;
    }

    private static List<String> sliceOf(List<String> keys, PartitionSlice slice) {
        if (slice == null) return keys;
        int from = (int) Math.min(slice.offset(), keys.size());
        int to = (int) Math.min(slice.offset() + slice.limit(), keys.size());
        return keys.subList(from, to);
    }

    private Optional<PartitionedRecord<GenericRecord>> recordAt(String layer, String key, Set<String> existingKeys,
                                                               PartitionKey read) {
        try {
            Optional<GenericRecord> record = S3DataLakeLayout.isBronze(layer)
                    ? envelopeAt(key, existingKeys)
                    : Optional.of(silverRecordAt(key, existingKeys));
            return record.map(found -> new PartitionedRecord<>(S3DataLakeLayout.partitionOf(layer, key), found));
        } catch (Exception e) {
            throw new DataLakeReadException(read, "the object " + bucket + "/" + key + " could not be read: " + e.getMessage(), e);
        }
    }

    private Optional<GenericRecord> envelopeAt(String key, Set<String> existingKeys) {
        if (S3DataLakeLayout.isFormerKey(key)) {
            return Optional.of(EnvelopeRecords.ofFormerFormat(get(key).asByteArray()));
        }
        ResponseBytes<GetObjectResponse> payload = get(key);
        Optional<EnvelopeHeader> header = S3EnvelopeHeaderMetadata.headerIn(payload.response().metadata())
                .or(() -> headerNextTo(key, existingKeys));
        if (header.isEmpty()) {
            LOG.warn("Skipped object {}/{}: it has no envelope header in its metadata or in {}", bucket, key,
                    S3DataLakeLayout.headerKeyNextTo(key));
        }
        return header.map(found -> EnvelopeRecords.of(found, payload.asByteArray()));
    }

    private GenericRecord silverRecordAt(String key, Set<String> existingKeys) {
        String headerKey = S3DataLakeLayout.headerKeyNextTo(key);
        return existingKeys.contains(headerKey)
                ? EnvelopeRecords.ofSeparated(get(headerKey).asByteArray(), get(key).asByteArray())
                : EnvelopeRecords.ofFormerFormat(get(key).asByteArray());
    }

    private Optional<EnvelopeHeader> headerNextTo(String payloadKey, Set<String> existingKeys) {
        String headerKey = S3DataLakeLayout.headerKeyNextTo(payloadKey);
        return existingKeys.contains(headerKey)
                ? Optional.of(HEADERS.deserialize(get(headerKey).asByteArray()))
                : Optional.empty();
    }

    private ResponseBytes<GetObjectResponse> get(String key) {
        return s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build());
    }
}

package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.S3ObjectKeys;

import java.util.List;

/**
 * Finds the objects of a partition of a layer, and of the partitions below it. The leading entries of the partition
 * are followed as a prefix while the bucket has objects under them. From the first one it has not, such as
 * {@code source=payments} kept behind the date entries, the objects below the last prefix are kept when their folder
 * has every remaining entry, wherever it is in their path.
 */
final class S3PartitionObjects {
    private final S3Client s3Client;
    private final String bucket;

    S3PartitionObjects(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    List<String> keysOf(String layer, PartitionKey partition) {
        List<String> entries = KeyEntries.split(partition.value());
        int leadingEntries = 0;
        while (leadingEntries < entries.size()
                && S3ObjectKeys.anyUnder(s3Client, bucket, S3DataLakeLayout.prefixOf(layer, entries.subList(0, leadingEntries + 1)))) {
            leadingEntries++;
        }
        List<String> remainingEntries = entries.subList(leadingEntries, entries.size());
        return S3ObjectKeys.under(s3Client, bucket, S3DataLakeLayout.prefixOf(layer, entries.subList(0, leadingEntries))).stream()
                .filter(key -> S3DataLakeLayout.isInAFolderWithEveryEntry(layer, key, remainingEntries))
                .toList();
    }
}

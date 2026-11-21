package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;

/**
 * Where the data lake keeps each envelope: the object {@code bronze/<partition>/<key>} of a bucket.
 */
final class S3DataLakeLayout {
    private static final String LAYER = "bronze";

    private S3DataLakeLayout() {
    }

    static String objectKeyOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return LAYER + "/" + String.join("/", KeyEntries.split(partition.value())) + "/" + idempotencyKey.value();
    }
}

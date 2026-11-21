package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.dataLake.PayloadFormat;

/**
 * Where the data lake keeps each envelope in a bucket, under {@code bronze/<partition>/}: its payload as it came in
 * {@code <key>.<extension>}, the extension given by its media type, and its header in {@code <key>.meta.json} when it
 * does not fit in the metadata of the payload object. An object {@code <key>} is a whole envelope in the former format.
 */
final class S3DataLakeLayout {
    private static final String LAYER = "bronze";
    private static final String SEPARATOR = "/";
    private static final String EXTENSION_SEPARATOR = ".";
    private static final String HEADER_SUFFIX = ".meta.json";

    private S3DataLakeLayout() {
    }

    static String payloadKeyOf(PartitionKey partition, EnvelopeHeader header) {
        return prefixOf(partition, header.idempotencyKey()) + PayloadFormat.extensionOf(header.mediaType());
    }

    static String headerKeyOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return formerKeyOf(partition, idempotencyKey) + HEADER_SUFFIX;
    }

    static String prefixOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return formerKeyOf(partition, idempotencyKey) + EXTENSION_SEPARATOR;
    }

    static String formerKeyOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return LAYER + SEPARATOR + String.join(SEPARATOR, KeyEntries.split(partition.value())) + SEPARATOR + idempotencyKey.value();
    }
}

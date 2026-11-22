package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.dataLake.PayloadFormat;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Where the data lake keeps each envelope in a bucket, under {@code bronze/<partition>/}: its payload as it came in
 * {@code <key>.<extension>}, the extension given by its media type, and its header in {@code <key>.meta.json} when it
 * does not fit in the metadata of the payload object. An object {@code <key>} is a whole envelope in the former format.
 * The key of an envelope has no dot, so the name of an object up to its first dot is the key of its envelope.
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

    static String prefixOf(PartitionKey partition) {
        return LAYER + SEPARATOR + KeyEntries.split(partition.value()).stream()
                .map(entry -> entry + SEPARATOR)
                .collect(Collectors.joining());
    }

    static PartitionKey partitionOf(String objectKey) {
        String folder = objectKey.substring(LAYER.length() + SEPARATOR.length(), objectKey.lastIndexOf(SEPARATOR) + 1);
        return new PartitionKey(KeyEntries.join(Arrays.stream(folder.split(SEPARATOR))
                .filter(entry -> !entry.isEmpty())
                .toList()));
    }

    static boolean isHeaderKey(String objectKey) {
        return objectKey.endsWith(HEADER_SUFFIX);
    }

    static boolean isFormerKey(String objectKey) {
        return !nameOf(objectKey).contains(EXTENSION_SEPARATOR);
    }

    static String headerKeyNextTo(String payloadKey) {
        String name = nameOf(payloadKey);
        String folder = payloadKey.substring(0, payloadKey.length() - name.length());
        return folder + name.substring(0, name.indexOf(EXTENSION_SEPARATOR)) + HEADER_SUFFIX;
    }

    private static String nameOf(String objectKey) {
        return objectKey.substring(objectKey.lastIndexOf(SEPARATOR) + 1);
    }
}

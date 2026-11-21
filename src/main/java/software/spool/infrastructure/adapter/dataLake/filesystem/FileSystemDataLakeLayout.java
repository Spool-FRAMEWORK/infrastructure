package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.dataLake.PayloadFormat;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Where the data lake keeps each envelope, one folder per partition entry under {@code bronze}: its payload as it
 * came in {@code <key>.<extension>}, the extension given by its media type, and its header in
 * {@code <key>.meta.json}. A {@code <key>.json} without a header next to it is a whole envelope in the former format.
 */
final class FileSystemDataLakeLayout {
    private static final String LAYER = "bronze";
    private static final String HEADER_SUFFIX = ".meta.json";
    private static final String EXTENSION_SEPARATOR = ".";

    private final Path basePath;

    FileSystemDataLakeLayout(String basePath) {
        this.basePath = Path.of(basePath);
    }

    Path folderOf(PartitionKey partition) {
        Path folder = basePath.resolve(LAYER);
        for (String entry : KeyEntries.split(partition.value())) {
            folder = folder.resolve(entry);
        }
        return folder;
    }

    Path headerFileOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return folderOf(partition).resolve(idempotencyKey.value() + HEADER_SUFFIX);
    }

    Path payloadFileOf(PartitionKey partition, EnvelopeHeader header) {
        return payloadFileNextTo(headerFileOf(partition, header.idempotencyKey()), header);
    }

    static boolean isFileOf(Path file, IdempotencyKey idempotencyKey) {
        return file.getFileName().toString().startsWith(idempotencyKey.value() + EXTENSION_SEPARATOR);
    }

    static boolean isHeaderFile(Path file) {
        return file.getFileName().toString().endsWith(HEADER_SUFFIX);
    }

    static Path payloadFileNextTo(Path headerFile, EnvelopeHeader header) {
        return headerFile.resolveSibling(keyOf(headerFile) + EXTENSION_SEPARATOR + PayloadFormat.extensionOf(header.mediaType()));
    }

    static boolean isPayloadOfAnEnvelope(Path file) {
        if (isHeaderFile(file)) return false;
        String name = file.getFileName().toString();
        int extension = name.lastIndexOf(EXTENSION_SEPARATOR);
        return extension > 0 && Files.exists(file.resolveSibling(name.substring(0, extension) + HEADER_SUFFIX));
    }

    private static String keyOf(Path headerFile) {
        String name = headerFile.getFileName().toString();
        return name.substring(0, name.length() - HEADER_SUFFIX.length());
    }
}

package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;

import java.nio.file.Path;

/**
 * Where the data lake keeps each envelope: {@code bronze/<partition>/<key>.json} under its root, one folder per
 * partition entry.
 */
final class FileSystemDataLakeLayout {
    private static final String LAYER = "bronze";
    private static final String FILE_EXTENSION = ".json";

    private final Path basePath;

    FileSystemDataLakeLayout(String basePath) {
        this.basePath = Path.of(basePath);
    }

    Path fileOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        Path folder = basePath.resolve(LAYER);
        for (String entry : KeyEntries.split(partition.value())) {
            folder = folder.resolve(entry);
        }
        return folder.resolve(idempotencyKey.value() + FILE_EXTENSION);
    }
}

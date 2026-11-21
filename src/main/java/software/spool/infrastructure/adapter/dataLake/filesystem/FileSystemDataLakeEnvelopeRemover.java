package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.exception.DataLakeRemovalException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@link DataLakeEnvelopeRemover} that deletes the files of each envelope, laid out by {@link FileSystemDataLakeLayout}:
 * first its header, so the envelope is gone at once, then its payload, or its single file in the former format.
 */
public class FileSystemDataLakeEnvelopeRemover implements DataLakeEnvelopeRemover {
    private final FileSystemDataLakeLayout layout;

    public FileSystemDataLakeEnvelopeRemover(String basePath) {
        this.layout = new FileSystemDataLakeLayout(basePath);
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        Path folder = layout.folderOf(partition);
        try {
            Files.deleteIfExists(layout.headerFileOf(partition, idempotencyKey));
            for (Path file : filesOf(folder, idempotencyKey)) {
                Files.deleteIfExists(file);
            }
        } catch (IOException e) {
            throw new DataLakeRemovalException("Envelope " + idempotencyKey.value() + " could not be deleted from " + folder, e);
        }
    }

    private static List<Path> filesOf(Path folder, IdempotencyKey idempotencyKey) throws IOException {
        if (!Files.isDirectory(folder)) return List.of();
        try (Stream<Path> files = Files.list(folder)) {
            return files.filter(file -> FileSystemDataLakeLayout.isFileOf(file, idempotencyKey)).toList();
        }
    }
}

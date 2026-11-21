package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.exception.DataLakeRemovalException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link DataLakeEnvelopeRemover} that deletes the file of each envelope, laid out by {@link FileSystemDataLakeLayout}.
 */
public class FileSystemDataLakeEnvelopeRemover implements DataLakeEnvelopeRemover {
    private final FileSystemDataLakeLayout layout;

    public FileSystemDataLakeEnvelopeRemover(String basePath) {
        this.layout = new FileSystemDataLakeLayout(basePath);
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        Path file = layout.fileOf(partition, idempotencyKey);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new DataLakeRemovalException("Envelope " + idempotencyKey.value() + " could not be deleted from " + file, e);
        }
    }
}

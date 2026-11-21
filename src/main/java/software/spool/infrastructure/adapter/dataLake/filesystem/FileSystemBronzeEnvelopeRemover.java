package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.exception.BronzeRemovalException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link BronzeEnvelopeRemover} that deletes the file of each envelope from the data lake, laid out by
 * {@link FileSystemBronzeLayout}.
 */
public class FileSystemBronzeEnvelopeRemover implements BronzeEnvelopeRemover {
    private final FileSystemBronzeLayout layout;

    public FileSystemBronzeEnvelopeRemover(String basePath) {
        this.layout = new FileSystemBronzeLayout(basePath);
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        Path file = layout.fileOf(partition, idempotencyKey);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new BronzeRemovalException("Envelope " + idempotencyKey.value() + " could not be deleted from " + file, e);
        }
    }
}

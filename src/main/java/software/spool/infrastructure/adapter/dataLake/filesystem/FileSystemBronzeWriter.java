package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.BronzeWriteException;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeWriter;
import software.spool.core.port.serde.RecordSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link BronzeWriter} that keeps each envelope in a file of the data lake, laid out by {@link FileSystemBronzeLayout}.
 */
public class FileSystemBronzeWriter implements BronzeWriter {
    private final FileSystemBronzeLayout layout;
    private final RecordSerializer<Envelope> serializer = RecordSerializerFactory.record();

    public FileSystemBronzeWriter(String basePath) {
        this.layout = new FileSystemBronzeLayout(basePath);
    }

    @Override
    public void save(PartitionKey partition, Envelope envelope) {
        Path file = layout.fileOf(partition, envelope.idempotencyKey());
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, serializer.serialize(envelope));
        } catch (IOException e) {
            throw new BronzeWriteException("Envelope " + envelope.idempotencyKey().value() + " could not be saved in " + file, e);
        }
    }
}

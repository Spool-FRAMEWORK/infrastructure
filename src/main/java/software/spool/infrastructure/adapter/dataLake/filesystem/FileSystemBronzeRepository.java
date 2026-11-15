package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.BronzeRepositoryException;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeRepository;
import software.spool.core.port.serde.RecordSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link BronzeRepository} that keeps each envelope in {@code bronze/<partition>/<key>.json} under the data lake
 * root, one folder per partition entry.
 */
public class FileSystemBronzeRepository implements BronzeRepository {
    private static final String LAYER = "bronze";
    private static final String FILE_EXTENSION = ".json";

    private final Path basePath;
    private final RecordSerializer<Envelope> serializer = RecordSerializerFactory.record();

    public FileSystemBronzeRepository(String basePath) {
        this.basePath = Path.of(basePath);
    }

    @Override
    public void save(PartitionKey partition, Envelope envelope) {
        Path file = fileOf(partition, envelope.idempotencyKey());
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, serializer.serialize(envelope));
        } catch (IOException e) {
            throw new BronzeRepositoryException("Envelope " + envelope.idempotencyKey().value() + " could not be saved in " + file, e);
        }
    }

    @Override
    public void delete(PartitionKey partition, IdempotencyKey idempotencyKey) {
        Path file = fileOf(partition, idempotencyKey);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new BronzeRepositoryException("Envelope " + idempotencyKey.value() + " could not be deleted from " + file, e);
        }
    }

    private Path fileOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        Path folder = basePath.resolve(LAYER);
        for (String entry : KeyEntries.split(partition.value())) {
            folder = folder.resolve(entry);
        }
        return folder.resolve(idempotencyKey.value() + FILE_EXTENSION);
    }
}

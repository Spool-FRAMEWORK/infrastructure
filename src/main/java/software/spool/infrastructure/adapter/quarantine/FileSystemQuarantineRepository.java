package software.spool.infrastructure.adapter.quarantine;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.QuarantineRepositoryException;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * {@link QuarantineRepository} that keeps each record in a JSON file named after the envelope's idempotency key.
 */
public class FileSystemQuarantineRepository implements QuarantineRepository {
    private static final String FILE_EXTENSION = ".json";

    private final Path basePath;
    private final RecordSerializer<QuarantinedRecord> serializer = RecordSerializerFactory.record();
    private final PayloadDeserializer<QuarantinedRecord> deserializer =
            PayloadDeserializerFactory.json().as(QuarantinedRecord.class);

    public FileSystemQuarantineRepository(String basePath) {
        this.basePath = Path.of(basePath);
    }

    @Override
    public void save(QuarantinedRecord quarantinedRecord) {
        try {
            Files.createDirectories(basePath);
            Files.write(fileOf(quarantinedRecord.idempotencyKey()), serializer.serialize(quarantinedRecord));
        } catch (IOException e) {
            throw new QuarantineRepositoryException(
                    "Quarantine record of " + quarantinedRecord.idempotencyKey().value() + " could not be saved in " + basePath, e);
        }
    }

    @Override
    public Optional<QuarantinedRecord> find(IdempotencyKey idempotencyKey) {
        Path file = fileOf(idempotencyKey);
        if (!Files.exists(file)) return Optional.empty();
        return Optional.of(read(file));
    }

    @Override
    public Collection<QuarantinedRecord> findAll() {
        if (!Files.exists(basePath)) return List.of();
        try (Stream<Path> files = Files.list(basePath)) {
            return files.filter(file -> file.getFileName().toString().endsWith(FILE_EXTENSION))
                    .map(this::read)
                    .toList();
        } catch (IOException e) {
            throw new QuarantineRepositoryException("Quarantine records could not be listed in " + basePath, e);
        }
    }

    @Override
    public void delete(IdempotencyKey idempotencyKey) {
        try {
            Files.deleteIfExists(fileOf(idempotencyKey));
        } catch (IOException e) {
            throw new QuarantineRepositoryException(
                    "Quarantine record of " + idempotencyKey.value() + " could not be deleted from " + basePath, e);
        }
    }

    private QuarantinedRecord read(Path file) {
        try {
            return deserializer.deserialize(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new QuarantineRepositoryException("Quarantine record could not be read from " + file, e);
        }
    }

    private Path fileOf(IdempotencyKey idempotencyKey) {
        return basePath.resolve(idempotencyKey.value() + FILE_EXTENSION);
    }
}

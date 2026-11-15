package software.spool.infrastructure.adapter.quarantine;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.QuarantineRepositoryException;
import software.spool.core.model.quarantine.QuarantineStatus;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * {@link QuarantineRepository} that keeps each record in a JSON file named after the envelope's idempotency key,
 * inside a folder named after the record's status. Resolving a record moves its file.
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
        IdempotencyKey idempotencyKey = quarantinedRecord.idempotencyKey();
        try {
            Path statusFolder = folderOf(quarantinedRecord.status());
            Files.createDirectories(statusFolder);
            Files.write(statusFolder.resolve(fileNameOf(idempotencyKey)), serializer.serialize(quarantinedRecord));
            for (QuarantineStatus otherStatus : QuarantineStatus.values()) {
                if (otherStatus != quarantinedRecord.status()) {
                    Files.deleteIfExists(fileOf(otherStatus, idempotencyKey));
                }
            }
        } catch (IOException e) {
            throw new QuarantineRepositoryException(
                    "Quarantine record of " + idempotencyKey.value() + " could not be saved in " + basePath, e);
        }
    }

    @Override
    public Optional<QuarantinedRecord> find(IdempotencyKey idempotencyKey) {
        return Arrays.stream(QuarantineStatus.values())
                .map(status -> fileOf(status, idempotencyKey))
                .filter(Files::exists)
                .findFirst()
                .map(this::read);
    }

    @Override
    public Collection<QuarantinedRecord> findByStatus(QuarantineStatus status) {
        Path statusFolder = folderOf(status);
        if (!Files.exists(statusFolder)) return List.of();
        try (Stream<Path> files = Files.list(statusFolder)) {
            return files.filter(file -> file.getFileName().toString().endsWith(FILE_EXTENSION))
                    .map(this::read)
                    .toList();
        } catch (IOException e) {
            throw new QuarantineRepositoryException("Quarantine records could not be listed in " + statusFolder, e);
        }
    }

    @Override
    public Collection<QuarantinedRecord> findAll() {
        return Arrays.stream(QuarantineStatus.values())
                .flatMap(status -> findByStatus(status).stream())
                .toList();
    }

    @Override
    public void delete(IdempotencyKey idempotencyKey) {
        try {
            for (QuarantineStatus status : QuarantineStatus.values()) {
                Files.deleteIfExists(fileOf(status, idempotencyKey));
            }
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

    private Path folderOf(QuarantineStatus status) {
        return basePath.resolve(status.name());
    }

    private Path fileOf(QuarantineStatus status, IdempotencyKey idempotencyKey) {
        return folderOf(status).resolve(fileNameOf(idempotencyKey));
    }

    private static String fileNameOf(IdempotencyKey idempotencyKey) {
        return idempotencyKey.value() + FILE_EXTENSION;
    }
}

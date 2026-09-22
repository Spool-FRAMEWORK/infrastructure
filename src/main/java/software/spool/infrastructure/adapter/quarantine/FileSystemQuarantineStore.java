package software.spool.infrastructure.adapter.quarantine;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.api.port.QuarantinedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class FileSystemQuarantineStore implements QuarantineStore {
    private final Path basePath;
    private final RecordSerializer<Object> serializer = RecordSerializerFactory.record();

    public FileSystemQuarantineStore(String basePath) {
        this.basePath = Path.of(basePath);
    }

    @Override
    public void send(QuarantinedRecord record) {
        try {
            Files.createDirectories(basePath);
            byte[] serialized = serializer.serialize(record);
            Files.write(basePath.resolve(UUID.randomUUID() + ".json"), serialized);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

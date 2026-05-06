package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.ingester.api.port.DataLakeWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class FileSystemDataLakeWriter implements DataLakeWriter {
    private final String path;
    private final RecordSerializer<Envelope> serializer = RecordSerializerFactory.record();

    public FileSystemDataLakeWriter(String path) {
        this.path = path;
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<Envelope> items) {
        try {
            Path dir = Path.of(path);
            Files.createDirectories(dir);
            return items.stream()
                    .map(this::writeEnvelope)
                    .filter(Objects::nonNull);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private IdempotencyKey writeEnvelope(Envelope envelope) {
        try {
            IdempotencyKey key = envelope.idempotencyKey();
            Path file = Path.of(path, key.value() + ".json");
            Files.writeString(file, serializer.serialize(envelope));
            return key;
        } catch (IOException e) {
            return null;
        }
    }
}
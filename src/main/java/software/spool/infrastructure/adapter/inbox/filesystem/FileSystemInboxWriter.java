package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.crawler.api.port.InboxWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSystemInboxWriter implements InboxWriter {
    private final String path;
    private final RecordSerializer<Envelope> serializer = RecordSerializerFactory.record();

    public FileSystemInboxWriter(String path) {
        this.path = path;
    }

    @Override
    public IdempotencyKey receive(Envelope envelope) throws InboxWriteException, DuplicateEventException {
        try {
            IdempotencyKey key = envelope.idempotencyKey();
            Path statusDir = Path.of(path, EnvelopeStatus.CAPTURED.name());
            Files.createDirectories(statusDir);
            Path dataFile = statusDir.resolve(key.value() + ".json");
            for (EnvelopeStatus status : EnvelopeStatus.values()) {
                if (Files.exists(Path.of(path, status.name(), key.value() + ".json"))) {
                    throw new DuplicateEventException(key);
                }
            }
            Files.writeString(dataFile, serializer.serialize(envelope));
            return key;
        } catch (IOException e) {
            throw new InboxWriteException(e.getMessage());
        }
    }
}
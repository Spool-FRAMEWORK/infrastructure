package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.crawler.api.port.InboxWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class FileSystemInboxWriter implements InboxWriter {
    private final String path;
    private final RecordSerializer<Envelope> serializer = RecordSerializerFactory.record();
    private final PayloadDeserializer<Envelope> deserializer = PayloadDeserializerFactory.json().as(Envelope.class);

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
            Files.write(dataFile, serializer.serialize(envelope));
            return key;
        } catch (IOException e) {
            throw new InboxWriteException(e.getMessage());
        }
    }

    /**
     * Writes the new envelope as captured before removing the one in quarantine, so a failure in between leaves both
     * rather than none.
     */
    @Override
    public Receipt receiveReplacingQuarantined(Envelope envelope) throws InboxWriteException, DuplicateEventException {
        Path quarantined = fileOf(EnvelopeStatus.QUARANTINED, envelope.idempotencyKey());
        if (!Files.exists(quarantined)) return Receipt.stored(receive(envelope));
        try {
            if (Arrays.equals(deserializer.deserialize(Files.readAllBytes(quarantined)).payload(), envelope.payload())) {
                throw new DuplicateEventException(envelope.idempotencyKey());
            }
            Path captured = fileOf(EnvelopeStatus.CAPTURED, envelope.idempotencyKey());
            Files.createDirectories(captured.getParent());
            Files.write(captured, serializer.serialize(envelope));
            Files.delete(quarantined);
            return Receipt.replacedQuarantined(envelope.idempotencyKey());
        } catch (IOException e) {
            throw new InboxWriteException(e.getMessage());
        }
    }

    private Path fileOf(EnvelopeStatus status, IdempotencyKey key) {
        return Path.of(path, status.name(), key.value() + ".json");
    }
}
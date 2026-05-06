package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxEnvelopeRemover;
import software.spool.core.port.serde.PayloadDeserializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileSystemInboxEnvelopeRemover implements InboxEnvelopeRemover {
    private final String path;
    private final PayloadDeserializer<Envelope> deserializer = PayloadDeserializerFactory.json().as(Envelope.class);

    public FileSystemInboxEnvelopeRemover(String path) {
        this.path = path;
    }

    @Override
    public Collection<Envelope> remove(Collection<IdempotencyKey> idempotencyKeys) {
        List<Envelope> removed = new ArrayList<>();
        for (IdempotencyKey key : idempotencyKeys) {
            for (EnvelopeStatus status : EnvelopeStatus.values()) {
                Path dataFile = Path.of(path, status.name(), key.value() + ".json");
                try {
                    if (Files.exists(dataFile)) {
                        Envelope envelope = deserializer.deserialize(Files.readString(dataFile));
                        Files.delete(dataFile);
                        removed.add(envelope);
                        break;
                    }
                } catch (IOException ignored) {}
            }
        }
        return removed;
    }
}
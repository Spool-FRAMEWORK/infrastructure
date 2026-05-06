package software.spool.infrastructure.adapter.inbox.filesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.spool.core.exception.InboxUpdateException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxUpdater;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileSystemInboxUpdater implements InboxUpdater {
    private final String path;
    private final ObjectMapper mapper = new ObjectMapper();

    public FileSystemInboxUpdater(String path) {
        this.path = path;
    }

    @Override
    public Collection<Envelope> update(Collection<IdempotencyKey> idempotencyKeys, EnvelopeStatus status) throws InboxUpdateException {
        List<Envelope> updated = new ArrayList<>();
        for (IdempotencyKey key : idempotencyKeys) {
            Path source = null;
            for (EnvelopeStatus s : EnvelopeStatus.values()) {
                Path candidate = Path.of(path, s.name(), key.value() + ".json");
                if (Files.exists(candidate)) {
                    source = candidate;
                    break;
                }
            }
            if (source == null) continue;
            try {
                Envelope envelope = mapper.readValue(source.toFile(), Envelope.class);
                Path targetDir = Path.of(path, status.name());
                Files.createDirectories(targetDir);
                Files.move(source, targetDir.resolve(key.value() + ".json"), StandardCopyOption.REPLACE_EXISTING);
                updated.add(envelope);
            } catch (IOException e) {
                throw new InboxUpdateException(idempotencyKeys, e.getMessage());
            }
        }
        return updated;
    }
}
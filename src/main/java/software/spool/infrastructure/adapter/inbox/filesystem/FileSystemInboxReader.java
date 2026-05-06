package software.spool.infrastructure.adapter.inbox.filesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.spool.core.exception.InboxReadException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class FileSystemInboxReader implements InboxReader {
    private final String path;
    private final ObjectMapper mapper = new ObjectMapper();

    public FileSystemInboxReader(String path) {
        this.path = path;
    }

    @Override
    public Optional<Envelope> findById(IdempotencyKey idempotencyKey) throws InboxReadException {
        try {
            for (EnvelopeStatus status : EnvelopeStatus.values()) {
                Path dataFile = Path.of(path, status.name(), idempotencyKey.value() + ".json");
                if (Files.exists(dataFile)) {
                    return Optional.of(mapper.readValue(dataFile.toFile(), Envelope.class));
                }
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new InboxReadException(e.getMessage());
        }
    }

    @Override
    public Collection<Envelope> findByIds(Collection<IdempotencyKey> idempotencyKeys) throws InboxReadException {
        return idempotencyKeys.stream().map(this::findById).filter(Optional::isPresent).map(Optional::get).toList();
    }

    @Override
    public Collection<Envelope> findByStatus(EnvelopeStatus status) throws InboxReadException {
        try {
            Path statusDir = Path.of(path, status.name());
            if (!Files.exists(statusDir)) return List.of();
            List<Envelope> result = new ArrayList<>();
            try (Stream<Path> files = Files.list(statusDir)) {
                for (Path file : files.filter(p -> p.toString().endsWith(".json")).toList()) {
                    result.add(mapper.readValue(file.toFile(), Envelope.class));
                }
            }
            return result;
        } catch (IOException e) {
            throw new InboxReadException(e.getMessage());
        }
    }
}
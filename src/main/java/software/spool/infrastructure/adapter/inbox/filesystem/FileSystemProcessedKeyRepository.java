package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.ProcessedKey;
import software.spool.core.port.idempotency.ProcessedKeyRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

/**
 * Keeps the processed keys next to the envelopes of a file system inbox, one file per key holding the moment it was
 * processed, so they outlive a restart and every replica that shares the folder sees them.
 */
public final class FileSystemProcessedKeyRepository implements ProcessedKeyRepository {
    static final String FOLDER = "processed-keys";

    private final Path folder;

    public FileSystemProcessedKeyRepository(String inboxPath) {
        this.folder = Path.of(inboxPath, FOLDER);
    }

    @Override
    public boolean contains(IdempotencyKey key) {
        return Files.exists(fileOf(key));
    }

    @Override
    public void save(ProcessedKey processedKey) {
        try {
            Files.createDirectories(folder);
            Files.writeString(fileOf(processedKey.key()), processedKey.processedAt().toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new InboxWriteException("The processed key " + processedKey.key().value() + " could not be kept: " + e.getMessage(), e);
        }
    }

    @Override
    public void forgetProcessedBefore(Instant moment) {
        if (!Files.isDirectory(folder)) return;
        try (Stream<Path> files = Files.list(folder)) {
            files.filter(file -> processedAt(file).isBefore(moment)).forEach(FileSystemProcessedKeyRepository::delete);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path fileOf(IdempotencyKey key) {
        return folder.resolve(key.value());
    }

    private static Instant processedAt(Path file) {
        try {
            return Instant.parse(Files.readString(file, StandardCharsets.UTF_8).strip());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void delete(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

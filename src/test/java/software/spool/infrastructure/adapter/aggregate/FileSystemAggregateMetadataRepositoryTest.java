package software.spool.infrastructure.adapter.aggregate;

import org.junit.jupiter.api.io.TempDir;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

class FileSystemAggregateMetadataRepositoryTest extends AggregateMetadataRepositoryContract {
    @TempDir
    Path folder;

    @Override
    AggregateMetadataRepository repository() {
        return new FileSystemAggregateMetadataRepository(folder);
    }

    @Override
    void keepAsIs(String key, byte[] content) {
        try {
            Path file = folder.resolve(key);
            Files.createDirectories(file.getParent());
            Files.write(file, content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

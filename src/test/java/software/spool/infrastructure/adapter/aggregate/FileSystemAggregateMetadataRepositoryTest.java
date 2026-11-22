package software.spool.infrastructure.adapter.aggregate;

import org.junit.jupiter.api.io.TempDir;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.nio.file.Path;

class FileSystemAggregateMetadataRepositoryTest extends AggregateMetadataRepositoryContract {
    @TempDir
    Path folder;

    @Override
    AggregateMetadataRepository repository() {
        return new FileSystemAggregateMetadataRepository(folder);
    }
}

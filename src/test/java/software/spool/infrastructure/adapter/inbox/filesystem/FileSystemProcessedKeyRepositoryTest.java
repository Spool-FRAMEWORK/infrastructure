package software.spool.infrastructure.adapter.inbox.filesystem;

import org.junit.jupiter.api.io.TempDir;
import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.infrastructure.adapter.inbox.ProcessedKeyRepositoryContract;

import java.nio.file.Path;

class FileSystemProcessedKeyRepositoryTest extends ProcessedKeyRepositoryContract {
    @TempDir
    Path inbox;

    @Override
    protected ProcessedKeyRepository repository() {
        return new FileSystemProcessedKeyRepository(inbox.toString());
    }
}

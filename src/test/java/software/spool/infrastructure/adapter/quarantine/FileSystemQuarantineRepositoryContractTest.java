package software.spool.infrastructure.adapter.quarantine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.port.quarantine.QuarantineRepository;

import java.nio.file.Path;

class FileSystemQuarantineRepositoryContractTest extends QuarantineRepositoryContract {

    @TempDir
    Path directory;

    private QuarantineRepository repository;

    @BeforeEach
    void createRepository() {
        repository = new FileSystemQuarantineRepository(directory.resolve("quarantine").toString());
    }

    @Override
    protected QuarantineRepository repository() {
        return repository;
    }
}

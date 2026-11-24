package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class FileSystemDataMartStoreTest extends DataMartStoreContract {
    @TempDir
    Path dataMart;

    @Override
    DataMartStore store() {
        return new FileSystemDataMartStore(dataMart);
    }
}

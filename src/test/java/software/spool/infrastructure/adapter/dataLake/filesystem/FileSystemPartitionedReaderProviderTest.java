package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.mounter.api.port.PartitionedReader;
import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemPartitionedReaderProviderTest extends AbstractPathBasedProviderTest<FileSystemPartitionedReaderProvider> {

    @Override
    protected FileSystemPartitionedReaderProvider createProvider() {
        return new FileSystemPartitionedReaderProvider();
    }

    @Override
    protected String expectedName() {
        return "FILE_SYSTEM";
    }

    @Override
    protected int expectedPriority() {
        return 10;
    }

    @Override
    protected Class<?> expectedProductType() {
        return PartitionedReader.class;
    }
}

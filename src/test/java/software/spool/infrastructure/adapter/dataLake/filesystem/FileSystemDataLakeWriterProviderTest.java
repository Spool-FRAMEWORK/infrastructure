package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemDataLakeWriterProviderTest extends AbstractPathBasedProviderTest<FileSystemDataLakeWriterProvider> {

    @Override
    protected FileSystemDataLakeWriterProvider createProvider() {
        return new FileSystemDataLakeWriterProvider();
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
        return DataLakeWriter.class;
    }
}

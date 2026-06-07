package software.spool.infrastructure.adapter.datamart;

import software.spool.mounter.api.port.DataMartWriter;
import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemDataMartWriterProviderTest extends AbstractPathBasedProviderTest<FileSystemDataMartWriterProvider> {

    @Override
    protected FileSystemDataMartWriterProvider createProvider() {
        return new FileSystemDataMartWriterProvider();
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
        return DataMartWriter.class;
    }
}

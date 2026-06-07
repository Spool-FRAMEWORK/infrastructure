package software.spool.infrastructure.adapter.datamart;

import software.spool.infrastructure.support.AbstractPathBasedProviderTest;
import software.spool.mounter.api.port.DataMartWriter;

class RawFileSystemDataMartWriterProviderTest extends AbstractPathBasedProviderTest<RawFileSystemDataMartWriterProvider> {

    @Override
    protected RawFileSystemDataMartWriterProvider createProvider() {
        return new RawFileSystemDataMartWriterProvider();
    }

    @Override
    protected String expectedName() {
        return "RAW_FILE_SYSTEM";
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

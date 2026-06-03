package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemInboxWriterProviderTest extends AbstractPathBasedProviderTest<FileSystemInboxWriterProvider> {

    @Override
    protected FileSystemInboxWriterProvider createProvider() {
        return new FileSystemInboxWriterProvider();
    }

    @Override
    protected String expectedName() {
        return "FILE_SYSTEM";
    }

    @Override
    protected int expectedPriority() {
        return 0;
    }

    @Override
    protected Class<?> expectedProductType() {
        return FileSystemInboxWriter.class;
    }
}

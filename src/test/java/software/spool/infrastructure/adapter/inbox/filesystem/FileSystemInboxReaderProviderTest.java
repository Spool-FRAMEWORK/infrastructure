package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemInboxReaderProviderTest extends AbstractPathBasedProviderTest<FileSystemInboxReaderProvider> {

    @Override
    protected FileSystemInboxReaderProvider createProvider() {
        return new FileSystemInboxReaderProvider();
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
        return FileSystemInboxReader.class;
    }
}

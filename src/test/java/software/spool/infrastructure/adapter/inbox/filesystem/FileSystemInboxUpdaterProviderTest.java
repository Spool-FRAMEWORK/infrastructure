package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemInboxUpdaterProviderTest extends AbstractPathBasedProviderTest<FileSystemInboxUpdaterProvider> {

    @Override
    protected FileSystemInboxUpdaterProvider createProvider() {
        return new FileSystemInboxUpdaterProvider();
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
        return FileSystemInboxUpdater.class;
    }
}

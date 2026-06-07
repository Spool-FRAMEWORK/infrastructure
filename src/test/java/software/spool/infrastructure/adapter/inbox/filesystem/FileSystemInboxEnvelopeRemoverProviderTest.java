package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.infrastructure.support.AbstractPathBasedProviderTest;

class FileSystemInboxEnvelopeRemoverProviderTest extends AbstractPathBasedProviderTest<FileSystemInboxEnvelopeRemoverProvider> {

    @Override
    protected FileSystemInboxEnvelopeRemoverProvider createProvider() {
        return new FileSystemInboxEnvelopeRemoverProvider();
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
        return FileSystemInboxEnvelopeRemover.class;
    }
}

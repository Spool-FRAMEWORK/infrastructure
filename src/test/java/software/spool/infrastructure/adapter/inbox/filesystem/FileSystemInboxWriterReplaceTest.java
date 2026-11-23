package software.spool.infrastructure.adapter.inbox.filesystem;

import org.junit.jupiter.api.io.TempDir;
import software.spool.core.port.inbox.InboxReader;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.crawler.api.port.InboxWriter;
import software.spool.infrastructure.adapter.inbox.InboxWriterReplaceContract;

import java.nio.file.Path;

class FileSystemInboxWriterReplaceTest extends InboxWriterReplaceContract {
    @TempDir
    Path inbox;

    @Override
    protected InboxWriter writer() {
        return new FileSystemInboxWriter(inbox.toString());
    }

    @Override
    protected InboxUpdater updater() {
        return new FileSystemInboxUpdater(inbox.toString());
    }

    @Override
    protected InboxReader reader() {
        return new FileSystemInboxReader(inbox.toString());
    }
}

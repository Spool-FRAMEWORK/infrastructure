package software.spool.infrastructure.adapter.inbox.filesystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.infrastructure.adapter.inbox.InboxUpdaterContract;

import java.nio.file.Path;
import java.util.Optional;

class FileSystemInboxUpdaterContractTest extends InboxUpdaterContract {

    @TempDir
    Path directory;

    private FileSystemInboxWriter writer;
    private FileSystemInboxUpdater updater;
    private FileSystemInboxReader reader;

    @BeforeEach
    void createAdapters() {
        writer = new FileSystemInboxWriter(directory.toString());
        updater = new FileSystemInboxUpdater(directory.toString());
        reader = new FileSystemInboxReader(directory.toString());
    }

    @Override
    protected InboxUpdater updater() {
        return updater;
    }

    @Override
    protected void store(Envelope envelope) throws Exception {
        writer.receive(envelope);
    }

    @Override
    protected Optional<Envelope> find(IdempotencyKey idempotencyKey) throws Exception {
        return reader.findById(idempotencyKey);
    }
}

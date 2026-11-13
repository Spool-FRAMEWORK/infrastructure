package software.spool.infrastructure.adapter.inbox.s3;

import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.infrastructure.adapter.inbox.InboxUpdaterContract;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.util.Optional;

class S3InboxUpdaterContractTest extends InboxUpdaterContract {

    private static final String BUCKET = "spool-inbox-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);
    private final S3InboxUpdater updater = new S3InboxUpdater(s3Client, BUCKET);
    private final S3InboxReader reader = new S3InboxReader(s3Client, BUCKET);

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

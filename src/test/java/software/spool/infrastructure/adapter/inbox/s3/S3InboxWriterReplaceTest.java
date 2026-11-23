package software.spool.infrastructure.adapter.inbox.s3;

import software.spool.core.port.inbox.InboxReader;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.crawler.api.port.InboxWriter;
import software.spool.infrastructure.adapter.inbox.InboxWriterReplaceContract;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

class S3InboxWriterReplaceTest extends InboxWriterReplaceContract {
    private static final String BUCKET = "spool-inbox-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Override
    protected InboxWriter writer() {
        return new S3InboxWriter(s3Client, BUCKET);
    }

    @Override
    protected InboxUpdater updater() {
        return new S3InboxUpdater(s3Client, BUCKET);
    }

    @Override
    protected InboxReader reader() {
        return new S3InboxReader(s3Client, BUCKET);
    }
}

package software.spool.infrastructure.adapter.inbox.s3;

import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.infrastructure.adapter.inbox.ProcessedKeyRepositoryContract;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

class S3ProcessedKeyRepositoryTest extends ProcessedKeyRepositoryContract {
    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Override
    protected ProcessedKeyRepository repository() {
        return new S3ProcessedKeyRepository(s3Client, "spool-inbox-test");
    }
}

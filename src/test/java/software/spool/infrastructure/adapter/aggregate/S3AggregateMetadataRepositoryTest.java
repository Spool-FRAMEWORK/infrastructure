package software.spool.infrastructure.adapter.aggregate;

import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.port.AggregateMetadataRepository;

class S3AggregateMetadataRepositoryTest extends AggregateMetadataRepositoryContract {
    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Override
    AggregateMetadataRepository repository() {
        return new S3AggregateMetadataRepository(s3Client, "spool-aggregates-test");
    }
}

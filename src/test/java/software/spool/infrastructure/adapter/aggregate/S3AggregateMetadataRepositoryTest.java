package software.spool.infrastructure.adapter.aggregate;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.port.AggregateMetadataRepository;

class S3AggregateMetadataRepositoryTest extends AggregateMetadataRepositoryContract {
    private static final String BUCKET = "spool-aggregates-test";
    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Override
    AggregateMetadataRepository repository() {
        return new S3AggregateMetadataRepository(s3Client, BUCKET);
    }

    @Override
    void keepAsIs(String key, byte[] content) {
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(content));
    }
}

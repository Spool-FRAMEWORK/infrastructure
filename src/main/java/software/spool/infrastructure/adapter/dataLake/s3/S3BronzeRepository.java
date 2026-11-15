package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.BronzeRepositoryException;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeRepository;

/**
 * {@link BronzeRepository} that keeps each envelope in the object {@code bronze/<partition>/<key>} of a bucket.
 */
public class S3BronzeRepository implements BronzeRepository {
    private static final String LAYER = "bronze";
    private static final String CONTENT_TYPE = "application/json";

    private final S3Client s3Client;
    private final String bucket;

    public S3BronzeRepository(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public void save(PartitionKey partition, Envelope envelope) {
        String key = objectKeyOf(partition, envelope.idempotencyKey());
        try {
            byte[] content = RecordSerializerFactory.record().serialize(envelope);
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(CONTENT_TYPE)
                            .contentLength((long) content.length)
                            .build(),
                    RequestBody.fromBytes(content));
        } catch (Exception e) {
            throw new BronzeRepositoryException("Envelope " + envelope.idempotencyKey().value() + " could not be saved in "
                    + bucket + "/" + key, e);
        }
    }

    @Override
    public void delete(PartitionKey partition, IdempotencyKey idempotencyKey) {
        String key = objectKeyOf(partition, idempotencyKey);
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (Exception e) {
            throw new BronzeRepositoryException("Envelope " + idempotencyKey.value() + " could not be deleted from "
                    + bucket + "/" + key, e);
        }
    }

    private static String objectKeyOf(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return LAYER + "/" + String.join("/", KeyEntries.split(partition.value())) + "/" + idempotencyKey.value();
    }
}

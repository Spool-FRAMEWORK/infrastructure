package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.BronzeWriteException;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeWriter;

/**
 * {@link BronzeWriter} that keeps each envelope in an object of a bucket, laid out by {@link S3BronzeLayout}.
 */
public class S3BronzeWriter implements BronzeWriter {
    private static final String CONTENT_TYPE = "application/json";

    private final S3Client s3Client;
    private final String bucket;

    public S3BronzeWriter(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public void save(PartitionKey partition, Envelope envelope) {
        String key = S3BronzeLayout.objectKeyOf(partition, envelope.idempotencyKey());
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
            throw new BronzeWriteException("Envelope " + envelope.idempotencyKey().value() + " could not be saved in "
                    + bucket + "/" + key, e);
        }
    }
}

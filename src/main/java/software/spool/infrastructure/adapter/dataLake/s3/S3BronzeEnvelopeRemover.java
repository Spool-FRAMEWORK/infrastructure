package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.spool.core.exception.BronzeRemovalException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;

/**
 * {@link BronzeEnvelopeRemover} that deletes the object of each envelope from a bucket, laid out by
 * {@link S3BronzeLayout}.
 */
public class S3BronzeEnvelopeRemover implements BronzeEnvelopeRemover {
    private final S3Client s3Client;
    private final String bucket;

    public S3BronzeEnvelopeRemover(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        String key = S3BronzeLayout.objectKeyOf(partition, idempotencyKey);
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (Exception e) {
            throw new BronzeRemovalException("Envelope " + idempotencyKey.value() + " could not be deleted from "
                    + bucket + "/" + key, e);
        }
    }
}

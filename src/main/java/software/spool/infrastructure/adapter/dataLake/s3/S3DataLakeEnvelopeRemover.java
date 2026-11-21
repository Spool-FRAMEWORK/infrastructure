package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.spool.core.exception.DataLakeRemovalException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;

/**
 * {@link DataLakeEnvelopeRemover} that deletes the object of each envelope from a bucket, laid out by
 * {@link S3DataLakeLayout}.
 */
public class S3DataLakeEnvelopeRemover implements DataLakeEnvelopeRemover {
    private final S3Client s3Client;
    private final String bucket;

    public S3DataLakeEnvelopeRemover(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        String key = S3DataLakeLayout.objectKeyOf(partition, idempotencyKey);
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (Exception e) {
            throw new DataLakeRemovalException("Envelope " + idempotencyKey.value() + " could not be deleted from "
                    + bucket + "/" + key, e);
        }
    }
}

package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.exception.DataLakeRemovalException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;

import java.util.List;

/**
 * {@link DataLakeEnvelopeRemover} that deletes the objects of each envelope from a bucket, laid out by
 * {@link S3DataLakeLayout}: first its header object, if it has one, then its payload, or its single object in the
 * former format.
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
        String prefix = S3DataLakeLayout.prefixOf(partition, idempotencyKey);
        try {
            delete(S3DataLakeLayout.headerKeyOf(partition, idempotencyKey));
            payloadKeys(prefix).forEach(this::delete);
            delete(S3DataLakeLayout.formerKeyOf(partition, idempotencyKey));
        } catch (Exception e) {
            throw new DataLakeRemovalException("Envelope " + idempotencyKey.value() + " could not be deleted from "
                    + bucket + "/" + prefix, e);
        }
    }

    private List<String> payloadKeys(String prefix) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
                .contents().stream()
                .map(S3Object::key)
                .toList();
    }

    private void delete(String key) {
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
    }
}

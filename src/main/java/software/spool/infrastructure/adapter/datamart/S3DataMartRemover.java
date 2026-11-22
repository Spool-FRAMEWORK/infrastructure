package software.spool.infrastructure.adapter.datamart;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartRemover;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

/**
 * Removes from a bucket a result that {@link S3DataMartWriter} wrote, finding its objects as {@link DataMartObject}
 * lays it out. It must serialize the results as the writer does.
 */
public class S3DataMartRemover implements DataMartRemover {
    private final S3Client s3Client;
    private final String bucket;
    private final RecordSerializer<Object> serializer;

    public S3DataMartRemover(S3Client s3Client, String bucket, RecordSerializer<Object> serializer) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.serializer = serializer;
    }

    public S3DataMartRemover(S3Client s3Client, String bucket) {
        this(s3Client, bucket, RecordSerializerFactory.record());
    }

    @Override
    public void remove(MountTarget target, PartitionedRecord<?> result) {
        DataMartObject.of(target, result, serializer).forEach(object -> s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(object.path())
                .build()));
    }
}

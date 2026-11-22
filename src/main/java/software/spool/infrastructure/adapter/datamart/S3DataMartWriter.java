package software.spool.infrastructure.adapter.datamart;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.stream.Stream;

/**
 * Writes the results of a mount to a bucket, the objects of each laid out as {@link DataMartObject} says, several at a
 * time on the threads of a {@link BoundedConcurrency}. The results are taken from the stream by the calling thread, so
 * the aggregator that produces them stays sequential.
 */
public class S3DataMartWriter implements DataMartWriter {
    private final S3Client s3Client;
    private final String bucket;
    private final RecordSerializer<Object> serializer;
    private final BoundedConcurrency concurrency;

    public S3DataMartWriter(S3Client s3Client, String bucket, BoundedConcurrency concurrency) {
        this(s3Client, bucket, RecordSerializerFactory.record(), concurrency);
    }

    public S3DataMartWriter(S3Client s3Client, String bucket, RecordSerializer<Object> serializer, BoundedConcurrency concurrency) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.serializer = serializer;
        this.concurrency = concurrency;
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        concurrency.forEach(result, partitioned -> DataMartObject.of(target, partitioned, serializer).forEach(this::put));
    }

    private void put(DataMartObject object) {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(object.path())
                        .contentLength((long) object.content().length)
                        .build(),
                RequestBody.fromBytes(object.content()));
    }
}

package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.vo.*;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.core.model.vo.PartitionedEnvelope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class S3DataLakeWriter implements DataLakeWriter {
    private static final Logger LOG = LoggerFactory.getLogger(S3DataLakeWriter.class);

    private static final String CONTENT_TYPE = "application/json";

    private final S3Client s3Client;
    private final String bucket;

    public S3DataLakeWriter(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> envelopes) {
        List<IdempotencyKey> written = new ArrayList<>();
        for (PartitionedEnvelope item : envelopes) {
            Envelope envelope = item.envelope();
            try {
                save(item.partition(), envelope);
                written.add(envelope.idempotencyKey());
            } catch (Exception e) {
                LOG.error("Skipped envelope {}: it could not be written to S3: {}", envelope.idempotencyKey().value(), e.getMessage());
            }
        }
        return written.stream();
    }

    private void save(PartitionKey partition, Envelope envelope) {
        byte[] content = RecordSerializerFactory.record().serialize(envelope);
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(S3DataLakeLayout.objectKeyOf(partition, envelope.idempotencyKey()))
                        .contentType(CONTENT_TYPE)
                        .contentLength((long) content.length)
                        .build(),
                RequestBody.fromBytes(content));
    }
}

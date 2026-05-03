package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.*;
import software.spool.ingester.api.port.DataLakeWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class S3DataLakeWriter implements DataLakeWriter {
    private static final String CONTENT_TYPE = "application/json";
    private final S3Client s3Client;
    private final String bucket;

    public S3DataLakeWriter(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<Envelope> envelopes) {
        List<IdempotencyKey> written = new ArrayList<>();
        for (Envelope envelope : envelopes) {
            try {
                byte[] payload = RecordSerializerFactory.record().serialize(envelope).getBytes(StandardCharsets.UTF_8);
                String key = "bronze/" + buildKey(envelope);
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType(CONTENT_TYPE)
                        .contentLength((long) payload.length)
                        .build();
                s3Client.putObject(request, RequestBody.fromBytes(payload));
                written.add(envelope.idempotencyKey());
            } catch (Exception e) {
                System.err.println(String.format("Failed to write envelope %s to S3, skipping", envelope.idempotencyKey()));
            }
        }
        return written.stream();
    }

    private String buildKey(Envelope item) {
        String partition = PartitionKey.of(getPartitionKeySchemaFrom(item)).from(item.payload()).value();
        String filename = item.idempotencyKey().value();
        return partition.replace("::", "/") + "/" + filename;
    }

    private static PartitionKeySchema getPartitionKeySchemaFrom(Envelope item) {
        return PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                .deserialize(item.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
    }
}
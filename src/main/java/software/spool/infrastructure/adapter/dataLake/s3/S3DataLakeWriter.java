package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.core.port.logging.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * {@link DataLakeWriter} that keeps each envelope as one object of a bucket, laid out by {@link S3DataLakeLayout}: its
 * payload as it came, with its media type as content type and its header as user metadata, in a single request. A
 * header too big for the user metadata goes to an object of its own, written after the payload.
 */
public class S3DataLakeWriter implements DataLakeWriter {
    private static final Logger LOG = LoggerFactory.getLogger(S3DataLakeWriter.class);
    private static final String HEADER_CONTENT_TYPE = "application/json";

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
        EnvelopeHeader header = envelope.header();
        Optional<Map<String, String>> headerMetadata = S3EnvelopeHeaderMetadata.metadataOf(header);
        put(S3DataLakeLayout.payloadKeyOf(partition, header), header.mediaType().value(), headerMetadata.orElse(Map.of()),
                envelope.payload());
        if (headerMetadata.isEmpty()) {
            put(S3DataLakeLayout.headerKeyOf(partition, header.idempotencyKey()), HEADER_CONTENT_TYPE, Map.of(),
                    RecordSerializerFactory.record().serialize(header));
        }
    }

    private void put(String key, String contentType, Map<String, String> metadata, byte[] content) {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType(contentType)
                        .metadata(metadata)
                        .contentLength((long) content.length)
                        .build(),
                RequestBody.fromBytes(content));
    }
}

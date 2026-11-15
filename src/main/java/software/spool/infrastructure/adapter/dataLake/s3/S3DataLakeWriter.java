package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.vo.*;
import software.spool.core.port.logging.Logger;
import software.spool.ingester.api.port.DataLakeWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class S3DataLakeWriter implements DataLakeWriter {
    private static final Logger LOG = LoggerFactory.getLogger(S3DataLakeWriter.class);

    private final S3BronzeRepository bronze;

    public S3DataLakeWriter(S3Client s3Client, String bucket) {
        this.bronze = new S3BronzeRepository(s3Client, bucket);
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<Envelope> envelopes) {
        List<IdempotencyKey> written = new ArrayList<>();
        for (Envelope envelope : envelopes) {
            try {
                bronze.save(PartitionKey.of(getPartitionKeySchemaFrom(envelope)).from(envelope.payload()), envelope);
                written.add(envelope.idempotencyKey());
            } catch (Exception e) {
                LOG.error("Skipped envelope {}: it could not be written to S3: {}", envelope.idempotencyKey().value(), e.getMessage());
            }
        }
        return written.stream();
    }

    private static PartitionKeySchema getPartitionKeySchemaFrom(Envelope item) {
        return PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                .deserialize(item.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
    }
}

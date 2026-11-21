package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.vo.*;
import software.spool.core.port.logging.Logger;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.PartitionedEnvelope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class S3DataLakeWriter implements DataLakeWriter {
    private static final Logger LOG = LoggerFactory.getLogger(S3DataLakeWriter.class);

    private final S3BronzeWriter bronze;

    public S3DataLakeWriter(S3Client s3Client, String bucket) {
        this.bronze = new S3BronzeWriter(s3Client, bucket);
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> envelopes) {
        List<IdempotencyKey> written = new ArrayList<>();
        for (PartitionedEnvelope item : envelopes) {
            Envelope envelope = item.envelope();
            try {
                bronze.save(item.partition(), envelope);
                written.add(envelope.idempotencyKey());
            } catch (Exception e) {
                LOG.error("Skipped envelope {}: it could not be written to S3: {}", envelope.idempotencyKey().value(), e.getMessage());
            }
        }
        return written.stream();
    }
}

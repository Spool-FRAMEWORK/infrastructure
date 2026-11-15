package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.exception.BronzeRepositoryException;
import software.spool.core.model.vo.*;
import software.spool.ingester.api.port.DataLakeWriter;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class FileSystemDataLakeWriter implements DataLakeWriter {
    private final FileSystemBronzeRepository bronze;

    public FileSystemDataLakeWriter(String path) {
        this.bronze = new FileSystemBronzeRepository(path);
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<Envelope> items) {
        return items.stream()
                .map(this::writeEnvelope)
                .filter(Objects::nonNull);
    }

    private IdempotencyKey writeEnvelope(Envelope envelope) {
        try {
            PartitionKeySchema partitionKeySchema = PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                    .deserialize(envelope.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
            bronze.save(PartitionKey.of(partitionKeySchema).from(envelope.payload()), envelope);
            return envelope.idempotencyKey();
        } catch (BronzeRepositoryException e) {
            return null;
        }
    }
}

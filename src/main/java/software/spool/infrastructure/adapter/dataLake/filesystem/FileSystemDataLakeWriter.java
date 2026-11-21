package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.exception.BronzeWriteException;
import software.spool.core.model.vo.*;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.PartitionedEnvelope;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class FileSystemDataLakeWriter implements DataLakeWriter {
    private final FileSystemBronzeWriter bronze;

    public FileSystemDataLakeWriter(String path) {
        this.bronze = new FileSystemBronzeWriter(path);
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> items) {
        return items.stream()
                .map(this::writeEnvelope)
                .filter(Objects::nonNull);
    }

    private IdempotencyKey writeEnvelope(PartitionedEnvelope item) {
        try {
            bronze.save(item.partition(), item.envelope());
            return item.envelope().idempotencyKey();
        } catch (BronzeWriteException e) {
            return null;
        }
    }
}

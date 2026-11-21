package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.core.port.serde.RecordSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * {@link DataLakeWriter} that keeps each envelope in a file, laid out by {@link FileSystemDataLakeLayout}.
 */
public class FileSystemDataLakeWriter implements DataLakeWriter {
    private final FileSystemDataLakeLayout layout;
    private final RecordSerializer<Envelope> serializer = RecordSerializerFactory.record();

    public FileSystemDataLakeWriter(String path) {
        this.layout = new FileSystemDataLakeLayout(path);
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> items) {
        return items.stream()
                .map(this::writeEnvelope)
                .filter(Objects::nonNull);
    }

    private IdempotencyKey writeEnvelope(PartitionedEnvelope item) {
        Path file = layout.fileOf(item.partition(), item.envelope().idempotencyKey());
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, serializer.serialize(item.envelope()));
            return item.envelope().idempotencyKey();
        } catch (IOException e) {
            return null;
        }
    }
}

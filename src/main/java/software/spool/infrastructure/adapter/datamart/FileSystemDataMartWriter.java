package software.spool.infrastructure.adapter.datamart;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

public class FileSystemDataMartWriter implements DataMartWriter {
    private final Path basePath;
    private final RecordSerializer<Object> serializer;

    public FileSystemDataMartWriter(Path basePath) {
        this.basePath = basePath;
        this.serializer = RecordSerializerFactory.record();
    }

    public FileSystemDataMartWriter(String basePath) {
        this(Path.of(basePath));
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        result.forEach(partitioned -> {
            String partitionPath = partitioned.partitionKey().value().replace("::", "/");
            Path dir = basePath.resolve(target.qualifiedDataMart()).resolve(partitionPath);
            try {
                Files.createDirectories(dir);
                byte[] serialized = serializer.serialize(partitioned.record());
                Path file = dir.resolve(IdempotencyKey.of(target.dataMart(), serialized).value() + "." + target.resolveExtension(serialized));
                Files.write(file, serialized);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
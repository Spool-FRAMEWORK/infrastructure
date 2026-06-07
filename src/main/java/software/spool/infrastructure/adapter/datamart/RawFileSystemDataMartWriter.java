package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class RawFileSystemDataMartWriter implements DataMartWriter {
    private final Path basePath;

    public RawFileSystemDataMartWriter(Path basePath) {
        this.basePath = basePath;
    }

    public RawFileSystemDataMartWriter(String basePath) {
        this(Path.of(basePath));
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        result.forEach(partitioned -> {
            String partitionPath = partitioned.partitionKey().value().replace("::", "/");
            Path dir = basePath.resolve(target.qualifiedDataMart()).resolve(partitionPath);
            try {
                Files.createDirectories(dir);
                Path file = dir.resolve(IdempotencyKey.of(target.dataMart(), partitioned.record().toString().getBytes()).value() + "." + target.resolveExtension((byte[]) partitioned.record()));
                Files.write(file, (byte[]) partitioned.record());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
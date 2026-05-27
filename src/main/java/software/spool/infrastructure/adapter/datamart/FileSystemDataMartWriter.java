package software.spool.infrastructure.adapter.datamart;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileSystemDataMartWriter implements DataMartWriter<GenericRecord> {
    private final Path basePath;
    private final ObjectMapper mapper;

    public FileSystemDataMartWriter(Path basePath) {
        this.basePath = basePath;
        this.mapper = new ObjectMapper();
    }

    public FileSystemDataMartWriter(String basePath) {
        this(Path.of(basePath));
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<GenericRecord>> result) {
        result.forEach(partitioned -> {
            String partitionPath = partitioned.partitionKey().value().replace("::", "/");
            Path dir = basePath.resolve(target.qualifiedDataMart()).resolve(partitionPath);
            try {
                Files.createDirectories(dir);
                Path file = dir.resolve("data.json");
                mapper.writeValue(file.toFile(), partitioned.record().toMap());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
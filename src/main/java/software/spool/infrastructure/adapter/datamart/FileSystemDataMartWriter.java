package software.spool.infrastructure.adapter.datamart;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Writes the records of a partition to the file system, several files at a time.
 *
 * <p>The records are taken from the stream by the calling thread, one at a time, so the aggregator that produces
 * them stays sequential. Only the writing of each file runs on the threads of a {@link BoundedConcurrency}.</p>
 */
public class FileSystemDataMartWriter implements DataMartWriter {
    private final Path basePath;
    private final RecordSerializer<Object> serializer;
    private final BoundedConcurrency concurrency;

    /**
     * Creates a writer that owns a pool with one thread per available processor.
     *
     * @param basePath the root of the data mart
     */
    public FileSystemDataMartWriter(Path basePath) {
        this(basePath, BoundedConcurrency.withThreads(Runtime.getRuntime().availableProcessors()));
    }

    /**
     * Creates a writer that writes on the given concurrency, which the caller closes.
     *
     * @param basePath    the root of the data mart
     * @param concurrency the pool and window the files are written with
     */
    public FileSystemDataMartWriter(Path basePath, BoundedConcurrency concurrency) {
        this(basePath, RecordSerializerFactory.record(), concurrency);
    }

    /**
     * Creates a writer with its own way of serializing the records.
     *
     * @param basePath    the root of the data mart
     * @param serializer  turns a record into the bytes of a file
     * @param concurrency the pool and window the files are written with
     */
    public FileSystemDataMartWriter(Path basePath, RecordSerializer<Object> serializer, BoundedConcurrency concurrency) {
        this.basePath = basePath;
        this.serializer = serializer;
        this.concurrency = concurrency;
    }

    public FileSystemDataMartWriter(String basePath) {
        this(Path.of(basePath));
    }

    public FileSystemDataMartWriter(String basePath, BoundedConcurrency concurrency) {
        this(Path.of(basePath), concurrency);
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        concurrency.forEach(result, partitioned -> {
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

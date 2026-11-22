package software.spool.infrastructure.adapter.datamart;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartRemover;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Removes from the file system a result that {@link FileSystemDataMartWriter} wrote, finding its files as
 * {@link DataMartObject} lays it out. It must serialize the results as the writer does.
 */
public class FileSystemDataMartRemover implements DataMartRemover {
    private final Path basePath;
    private final RecordSerializer<Object> serializer;

    public FileSystemDataMartRemover(Path basePath, RecordSerializer<Object> serializer) {
        this.basePath = basePath;
        this.serializer = serializer;
    }

    public FileSystemDataMartRemover(String basePath) {
        this(Path.of(basePath), RecordSerializerFactory.record());
    }

    @Override
    public void remove(MountTarget target, PartitionedRecord<?> result) {
        DataMartObject.of(target, result, serializer).forEach(object -> delete(basePath.resolve(object.path())));
    }

    private static void delete(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Result could not be removed from " + file, e);
        }
    }
}

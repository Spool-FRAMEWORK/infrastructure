package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.DeserializationException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class FileSystemPartitionedReader implements PartitionedReader {
    private final Path basePath;
    private final PayloadDeserializer<GenericRecord> deserializer;

    public FileSystemPartitionedReader(Path basePath, PayloadDeserializer<GenericRecord> deserializer) {
        this.basePath = basePath;
        this.deserializer = deserializer;
    }

    public FileSystemPartitionedReader(String basePath, PayloadDeserializer<GenericRecord> deserializer) {
        this(Paths.get(basePath), deserializer);
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        Path searchPath = basePath.resolve(mountTarget.mode() == MountMode.TRANSFORMATION ? "bronze" : "silver");
        Map<String, String> constraints = parseConstraints(mountTarget.sourceKey());
        if (!Files.exists(searchPath)) return Stream.of();
        try (Stream<Path> walk = Files.walk(searchPath)) {
            LoggerFactory.getLogger("FileSystemPartitionedReader").info("Searching for " + searchPath + " in " + mountTarget.sourceKey().value());
            List<PartitionedRecord<GenericRecord>> read = walk
                    .filter(Files::isRegularFile)
                    .filter(file -> matches(searchPath.relativize(file), constraints))
                    .flatMap(file -> toRecord(file, searchPath))
                    .toList();
            LoggerFactory.getLogger("FileSystemPartitionedReader").info("Found " + read.size() + " records");
            return read.stream();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to walk path: " + searchPath, e);
        }
    }

    private boolean matches(Path relativePath, Map<String, String> constraints) {
        Map<String, String> segments = new HashMap<>();
        for (int i = 0; i < relativePath.getNameCount(); i++) {
            String segment = relativePath.getName(i).toString();
            int eq = segment.indexOf('=');
            if (eq > 0) {
                segments.put(segment.substring(0, eq), segment.substring(eq + 1));
            }
        }
        for (Map.Entry<String, String> constraint : constraints.entrySet()) {
            String actual = segments.get(constraint.getKey());
            if (!constraint.getValue().equals(actual)) {
                return false;
            }
        }
        return true;
    }

    private Stream<PartitionedRecord<GenericRecord>> toRecord(Path file, Path searchPath) {
        try {
            byte[] bytes = Files.readAllBytes(file);
            GenericRecord record = deserializer.deserialize(bytes);
            PartitionKey fileKey = resolveFileKey(file, searchPath);
            return Stream.of(new PartitionedRecord<>(fileKey, record));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read file: " + file, e);
        } catch (DeserializationException e) {
            throw new RuntimeException("Failed to deserialize file: " + file + ". " + e.getMessage(), e);
        }
    }

    private PartitionKey resolveFileKey(Path file, Path searchPath) {
        Path relative = searchPath.relativize(file.getParent());
        String[] segments = new String[relative.getNameCount()];
        for (int i = 0; i < relative.getNameCount(); i++) {
            segments[i] = relative.getName(i).toString();
        }
        return new PartitionKey(String.join("::", segments));
    }

    private static Map<String, String> parseConstraints(PartitionKey partitionKey) {
        Map<String, String> constraints = new HashMap<>();
        for (String segment : partitionKey.value().split("::")) {
            int eq = segment.indexOf('=');
            if (eq > 0) {
                constraints.put(segment.substring(0, eq), segment.substring(eq + 1));
            }
        }
        return constraints;
    }
}

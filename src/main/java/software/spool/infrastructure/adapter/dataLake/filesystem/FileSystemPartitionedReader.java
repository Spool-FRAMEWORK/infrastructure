package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.DeserializationException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.scaling.PartitionSlice;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reads the records of a partition from the file system, several files at a time.
 *
 * <p>The files are read on the threads of a {@link BoundedConcurrency}, not on the common pool of the JVM. The
 * stream it returns is sequential and in the order of the files, so the aggregator that consumes it sees exactly
 * what a single-threaded read would give it.</p>
 */
public class FileSystemPartitionedReader implements PartitionedReader {

    private static final Logger log = LoggerFactory.getLogger("FileSystemPartitionedReader");

    private final Path basePath;
    private final PayloadDeserializer<GenericRecord> deserializer;
    private final BoundedConcurrency concurrency;

    /**
     * Creates a reader that owns a pool with one thread per available processor.
     *
     * @param basePath     the root of the data lake
     * @param deserializer turns the bytes of a file into a record
     */
    public FileSystemPartitionedReader(Path basePath, PayloadDeserializer<GenericRecord> deserializer) {
        this(basePath, deserializer, BoundedConcurrency.withThreads(Runtime.getRuntime().availableProcessors()));
    }

    /**
     * Creates a reader that reads on the given concurrency, which the caller closes.
     *
     * @param basePath     the root of the data lake
     * @param deserializer turns the bytes of a file into a record
     * @param concurrency  the pool and window the files are read with
     */
    public FileSystemPartitionedReader(Path basePath, PayloadDeserializer<GenericRecord> deserializer,
                                       BoundedConcurrency concurrency) {
        this.basePath = basePath;
        this.deserializer = deserializer;
        this.concurrency = concurrency;
    }

    public FileSystemPartitionedReader(String basePath, PayloadDeserializer<GenericRecord> deserializer) {
        this(Paths.get(basePath), deserializer);
    }

    public FileSystemPartitionedReader(String basePath, PayloadDeserializer<GenericRecord> deserializer,
                                       BoundedConcurrency concurrency) {
        this(Paths.get(basePath), deserializer, concurrency);
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        Path layerPath = basePath.resolve(mountTarget.mode() == MountMode.TRANSFORMATION ? "bronze" : "silver");
        Map<String, String> constraints = parseConstraints(mountTarget.sourceKey());
        ResolutionResult resolved = resolveConstrainedPath(layerPath, constraints);

        log.info("Resolved search path: {} (pending constraints: {})", resolved.path(), resolved.pending());

        if (!Files.exists(resolved.path())) {
            log.warn("Path does not exist, returning empty: {}", resolved.path());
            return Stream.of();
        }

        List<Path> files = collectFiles(resolved);
        List<Path> slice = applySlice(files, mountTarget.slice());

        log.info("Reading {} files (total in partition: {})", slice.size(), files.size());

        int total = slice.size();
        AtomicLong counter = new AtomicLong(0);
        return concurrency.map(slice, file -> {
                    long count = counter.incrementAndGet();
                    if (count % 100 == 0) log.info("Streamed {}/{} files...", count, total);
                    return toRecord(file, resolved.path());
                })
                .onClose(() -> log.info("Read complete — {} files streamed", counter.get()));
    }

    private List<Path> collectFiles(ResolutionResult resolved) {
        try (Stream<Path> walk = Files.walk(resolved.path())) {
            return walk
                    .filter(Files::isRegularFile)
                    .filter(file -> matchesPendingConstraints(file, resolved.pending()))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to walk path: {}", resolved.path(), e);
            throw new UncheckedIOException("Failed to walk path: " + resolved.path(), e);
        }
    }

    private List<Path> applySlice(List<Path> files, PartitionSlice slice) {
        if (slice == null) return files;
        int from = (int) slice.offset();
        int to = (int) Math.min(slice.offset() + slice.limit(), files.size());
        return files.subList(from, to);
    }

    record ResolutionResult(Path path, Map<String, String> pending) {}

    static ResolutionResult resolveConstrainedPath(Path base, Map<String, String> constraints) {
        Path resolved = base;
        Map<String, String> pending = new LinkedHashMap<>();
        boolean skipping = false;
        for (Map.Entry<String, String> entry : constraints.entrySet()) {
            if (!skipping) {
                Path candidate = resolved.resolve(entry.getKey() + "=" + entry.getValue());
                if (Files.isDirectory(candidate)) {
                    resolved = candidate;
                } else {
                    skipping = true;
                    pending.put(entry.getKey(), entry.getValue());
                }
            } else {
                pending.put(entry.getKey(), entry.getValue());
            }
        }
        return new ResolutionResult(resolved, pending);
    }

    static boolean matchesPendingConstraints(Path file, Map<String, String> pending) {
        if (pending.isEmpty()) return true;
        String normalised = file.toString().replace('\\', '/');
        return pending.entrySet().stream()
                .allMatch(e -> normalised.contains("/" + e.getKey() + "=" + e.getValue() + "/"));
    }

    private PartitionedRecord<GenericRecord> toRecord(Path file, Path searchPath) {
        try {
            byte[] bytes = Files.readAllBytes(file);
            GenericRecord record = deserializer.deserialize(bytes);
            PartitionKey fileKey = resolveFileKey(file, searchPath);
            return new PartitionedRecord<>(fileKey, record);
        } catch (IOException e) {
            log.error("Failed to read file: {}", file, e);
            throw new UncheckedIOException("Failed to read file: " + file, e);
        } catch (DeserializationException e) {
            log.error("Failed to deserialize file: {} — {}", file, e.getMessage(), e);
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

    static Map<String, String> parseConstraints(PartitionKey partitionKey) {
        Map<String, String> constraints = new LinkedHashMap<>();
        for (String segment : partitionKey.value().split("::")) {
            int eq = segment.indexOf('=');
            if (eq > 0) {
                constraints.put(segment.substring(0, eq), segment.substring(eq + 1));
            }
        }
        return constraints;
    }
}

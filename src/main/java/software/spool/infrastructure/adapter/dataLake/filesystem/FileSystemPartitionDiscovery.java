package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.logging.Logger;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

public class FileSystemPartitionDiscovery implements PartitionDiscovery {

    private static final Logger log = LoggerFactory.getLogger("FileSystemPartitionDiscovery");

    private final Path basePath;
    private final MountMode mode;

    public FileSystemPartitionDiscovery(Path basePath, MountMode mode) {
        this.basePath = basePath;
        this.mode = mode;
    }

    public FileSystemPartitionDiscovery(String basePath, MountMode mode) {
        this(Paths.get(basePath), mode);
    }

    @Override
    public List<PartitionInfo> discover(PartitionKey scope) {
        Path layerPath = basePath.resolve(mode == MountMode.TRANSFORMATION ? "bronze" : "silver");
        Map<String, String> constraints = FileSystemPartitionedReader.parseConstraints(scope);
        FileSystemPartitionedReader.ResolutionResult resolved =
                FileSystemPartitionedReader.resolveConstrainedPath(layerPath, constraints);

        log.info("Discovering partitions — scope: {}, search path: {}", scope.value(), resolved.path());

        if (!Files.exists(resolved.path())) {
            log.warn("Search path does not exist, no partitions found: {}", resolved.path());
            return List.of();
        }

        List<PartitionInfo> partitions;
        try (Stream<Path> walk = Files.walk(resolved.path())) {
            partitions = walk
                    .filter(Files::isDirectory)
                    .filter(this::containsFiles)
                    .filter(dir -> FileSystemPartitionedReader.matchesPendingConstraints(dir, resolved.pending()))
                    .map(dir -> {
                        long count = countFiles(dir);
                        PartitionKey key = toPartitionKey(dir, layerPath);
                        log.info("  Found partition: {} ({} files)", key.value(), count);
                        return new PartitionInfo(key, OptionalLong.of(count), OptionalLong.empty());
                    })
                    .toList();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Failed to discover partitions under: " + resolved.path(), e);
        }

        log.info("Discovery complete — {} partition(s) found", partitions.size());
        return partitions;
    }

    private boolean containsFiles(Path dir) {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.anyMatch(Files::isRegularFile);
        } catch (IOException e) {
            return false;
        }
    }

    private long countFiles(Path dir) {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.filter(Files::isRegularFile).count();
        } catch (IOException e) {
            return 0L;
        }
    }

    private PartitionKey toPartitionKey(Path dir, Path layerPath) {
        Path relative = layerPath.relativize(dir);
        String[] segments = new String[relative.getNameCount()];
        for (int i = 0; i < relative.getNameCount(); i++) {
            segments[i] = relative.getName(i).toString();
        }
        return new PartitionKey(String.join("::", segments));
    }
}

package software.spool.infrastructure.adapter.filesystem;

import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthProbe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Checks that the folder of a piece on the file system exists and can be written, by writing an empty file and
 * deleting it.
 */
public final class FileSystemHealthProbe implements HealthProbe {
    private static final String NAME = "file system";
    private static final String PROBE_PREFIX = ".spool-health-";
    private static final String PROBE_SUFFIX = ".tmp";

    private final Path folder;

    public FileSystemHealthProbe(Path folder) {
        this.folder = folder;
    }

    @Override
    public HealthCheck probe() {
        if (!Files.isDirectory(folder)) {
            return HealthCheck.unhealthy(NAME, "The folder " + folder + " does not exist");
        }
        try {
            Files.delete(Files.createTempFile(folder, PROBE_PREFIX, PROBE_SUFFIX));
            return HealthCheck.healthy(NAME);
        } catch (IOException e) {
            return HealthCheck.unhealthy(NAME, "The folder " + folder + " cannot be written: " + e.getMessage());
        }
    }
}

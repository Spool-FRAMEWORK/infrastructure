package software.spool.infrastructure.adapter.filesystem;

import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;

/**
 * The check of a data mart of type {@code RAW_FILE_SYSTEM}, the same as one on the file system.
 */
@SpoolPlugin(HealthProbeProvider.class)
public class RawFileSystemHealthProbeProvider extends FileSystemHealthProbeProvider {
    @Override
    public String name() {
        return "RAW_FILE_SYSTEM";
    }
}

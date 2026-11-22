package software.spool.infrastructure.adapter.filesystem;

import software.spool.core.port.health.HealthProbe;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;

import java.nio.file.Path;

@SpoolPlugin(HealthProbeProvider.class)
public class FileSystemHealthProbeProvider implements HealthProbeProvider {
    private static final String PATH = "path";

    @Override
    public String name() {
        return "FILE_SYSTEM";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has(PATH);
    }

    @Override
    public HealthProbe create(PluginConfiguration configuration) {
        return new FileSystemHealthProbe(Path.of(configuration.require(PATH)));
    }
}

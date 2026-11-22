package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.PartitionDiscoveryProvider;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;

@SpoolPlugin(PartitionDiscoveryProvider.class)
public class FileSystemPartitionDiscoveryProvider implements PartitionDiscoveryProvider {
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
        return configuration.has("path");
    }

    @Override
    public PartitionDiscovery create(PluginConfiguration configuration) {
        return new FileSystemPartitionDiscovery(configuration.require("path"), configuration.require(MODE, MountMode.class));
    }
}

package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.PartitionDiscoveryProvider;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;

@SpoolPlugin(PartitionDiscoveryProvider.class)
public class S3PartitionDiscoveryProvider implements PartitionDiscoveryProvider {
    @Override
    public String name() {
        return "S3";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("region") &&
                configuration.has("bucket") &&
                configuration.has("endpoint");
    }

    @Override
    public PartitionDiscovery create(PluginConfiguration configuration) {
        return new S3PartitionDiscovery(S3ClientFactory.create(configuration), configuration.require("bucket"),
                configuration.require(MODE, MountMode.class));
    }
}

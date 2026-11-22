package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.infrastructure.adapter.concurrency.ConcurrencyConfiguration;
import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.mounter.api.port.PartitionedReader;

@SpoolPlugin(PartitionedReaderProvider.class)
public class S3PartitionedReaderProvider implements PartitionedReaderProvider {
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
    public PartitionedReader create(PluginConfiguration configuration) {
        return new S3PartitionedReader(
                S3ClientFactory.create(configuration),
                configuration.require("bucket"),
                ConcurrencyConfiguration.from(configuration));
    }
}

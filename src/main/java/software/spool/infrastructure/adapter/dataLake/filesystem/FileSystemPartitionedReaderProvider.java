package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.infrastructure.adapter.concurrency.ConcurrencyConfiguration;
import software.spool.infrastructure.adapter.dataLake.EnvelopeRecords;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.mounter.api.port.PartitionedReader;

@SpoolPlugin(PartitionedReaderProvider.class)
public class FileSystemPartitionedReaderProvider implements PartitionedReaderProvider {
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
    public PartitionedReader create(PluginConfiguration configuration) {
        return new FileSystemPartitionedReader(
                configuration.require("path"),
                EnvelopeRecords::ofFormerFormat,
                ConcurrencyConfiguration.from(configuration)
        );
    }
}

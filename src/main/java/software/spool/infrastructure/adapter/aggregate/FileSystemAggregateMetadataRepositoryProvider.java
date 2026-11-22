package software.spool.infrastructure.adapter.aggregate;

import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.aggregate.AggregateMetadataRepositoryProvider;
import software.spool.mounter.api.port.AggregateMetadataRepository;

@SpoolPlugin(AggregateMetadataRepositoryProvider.class)
public class FileSystemAggregateMetadataRepositoryProvider implements AggregateMetadataRepositoryProvider {
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
    public AggregateMetadataRepository create(PluginConfiguration configuration) {
        return new FileSystemAggregateMetadataRepository(configuration.require("path"));
    }
}

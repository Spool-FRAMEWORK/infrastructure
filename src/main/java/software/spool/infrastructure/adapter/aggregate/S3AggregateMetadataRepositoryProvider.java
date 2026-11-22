package software.spool.infrastructure.adapter.aggregate;

import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.aggregate.AggregateMetadataRepositoryProvider;
import software.spool.mounter.api.port.AggregateMetadataRepository;

@SpoolPlugin(AggregateMetadataRepositoryProvider.class)
public class S3AggregateMetadataRepositoryProvider implements AggregateMetadataRepositoryProvider {
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
    public AggregateMetadataRepository create(PluginConfiguration configuration) {
        return new S3AggregateMetadataRepository(S3ClientFactory.create(configuration), configuration.require("bucket"));
    }
}

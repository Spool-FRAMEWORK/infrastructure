package software.spool.infrastructure.adapter.inbox.s3;

import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.inbox.ProcessedKeyRepositoryProvider;

@SpoolPlugin(ProcessedKeyRepositoryProvider.class)
public class S3ProcessedKeyRepositoryProvider implements ProcessedKeyRepositoryProvider {
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
        return configuration.has("region") && configuration.has("bucket") && configuration.has("endpoint");
    }

    @Override
    public ProcessedKeyRepository create(PluginConfiguration configuration) {
        return new S3ProcessedKeyRepository(S3ClientFactory.create(configuration), configuration.require("bucket"));
    }
}

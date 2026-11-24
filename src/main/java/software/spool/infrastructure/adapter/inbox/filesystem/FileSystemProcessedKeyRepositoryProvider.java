package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.inbox.ProcessedKeyRepositoryProvider;

@SpoolPlugin(ProcessedKeyRepositoryProvider.class)
public class FileSystemProcessedKeyRepositoryProvider implements ProcessedKeyRepositoryProvider {
    @Override
    public String name() {
        return "FILE_SYSTEM";
    }

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("path");
    }

    @Override
    public ProcessedKeyRepository create(PluginConfiguration configuration) {
        return new FileSystemProcessedKeyRepository(configuration.require("path"));
    }
}

package software.spool.infrastructure.adapter.quarantine;

import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.quarantine.QuarantineRepositoryProvider;

@SpoolPlugin(QuarantineRepositoryProvider.class)
public class FileSystemQuarantineRepositoryProvider implements QuarantineRepositoryProvider {
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
    public QuarantineRepository create(PluginConfiguration configuration) {
        return new FileSystemQuarantineRepository(configuration.require("path"));
    }
}

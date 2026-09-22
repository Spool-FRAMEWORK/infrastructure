package software.spool.infrastructure.adapter.quarantine;

import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.quarantine.QuarantineStoreProvider;
import software.spool.ingester.api.port.QuarantineStore;

@SpoolPlugin(QuarantineStoreProvider.class)
public class FileSystemQuarantineStoreProvider implements QuarantineStoreProvider {
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
    public QuarantineStore create(PluginConfiguration configuration) {
        return new FileSystemQuarantineStore(configuration.require("path"));
    }
}

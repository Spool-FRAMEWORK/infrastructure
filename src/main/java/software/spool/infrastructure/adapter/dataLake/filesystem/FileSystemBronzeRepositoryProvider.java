package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.port.datalake.BronzeRepository;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.BronzeRepositoryProvider;

@SpoolPlugin(BronzeRepositoryProvider.class)
public class FileSystemBronzeRepositoryProvider implements BronzeRepositoryProvider {
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
    public BronzeRepository create(PluginConfiguration configuration) {
        return new FileSystemBronzeRepository(configuration.require("path"));
    }
}

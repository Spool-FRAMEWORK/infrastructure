package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.port.datalake.BronzeWriter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.BronzeWriterProvider;

@SpoolPlugin(BronzeWriterProvider.class)
public class FileSystemBronzeWriterProvider implements BronzeWriterProvider {
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
    public BronzeWriter create(PluginConfiguration configuration) {
        return new FileSystemBronzeWriter(configuration.require("path"));
    }
}

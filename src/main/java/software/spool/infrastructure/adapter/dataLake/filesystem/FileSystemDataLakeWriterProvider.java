package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.ingester.api.port.DataLakeWriter;

@SpoolPlugin(DataLakeWriterProvider.class)
public class FileSystemDataLakeWriterProvider implements DataLakeWriterProvider {
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
    public DataLakeWriter create(PluginConfiguration configuration) {
        return new FileSystemDataLakeWriter(configuration.require("path"));
    }
}
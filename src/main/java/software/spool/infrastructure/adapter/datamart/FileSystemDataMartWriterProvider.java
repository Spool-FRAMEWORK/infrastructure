package software.spool.infrastructure.adapter.datamart;

import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.mounter.api.port.DataMartWriter;

@SpoolPlugin(DataMartWriterProvider.class)
public class FileSystemDataMartWriterProvider implements DataMartWriterProvider {
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
    public DataMartWriter create(PluginConfiguration configuration) {
        return new FileSystemDataMartWriter(configuration.require("path"));
    }
}

package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeEnvelopeRemoverProvider;

@SpoolPlugin(DataLakeEnvelopeRemoverProvider.class)
public class FileSystemDataLakeEnvelopeRemoverProvider implements DataLakeEnvelopeRemoverProvider {
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
    public DataLakeEnvelopeRemover create(PluginConfiguration configuration) {
        return new FileSystemDataLakeEnvelopeRemover(configuration.require("path"));
    }
}

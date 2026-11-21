package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.BronzeEnvelopeRemoverProvider;

@SpoolPlugin(BronzeEnvelopeRemoverProvider.class)
public class FileSystemBronzeEnvelopeRemoverProvider implements BronzeEnvelopeRemoverProvider {
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
    public BronzeEnvelopeRemover create(PluginConfiguration configuration) {
        return new FileSystemBronzeEnvelopeRemover(configuration.require("path"));
    }
}

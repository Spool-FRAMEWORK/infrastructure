package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.port.inbox.InboxEnvelopeRemover;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(InboxEnvelopeRemoverProvider.class)
public class FileSystemInboxEnvelopeRemoverProvider implements InboxEnvelopeRemoverProvider {
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
    public InboxEnvelopeRemover create(PluginConfiguration configuration) {
        return new FileSystemInboxEnvelopeRemover(configuration.require("path"));
    }
}
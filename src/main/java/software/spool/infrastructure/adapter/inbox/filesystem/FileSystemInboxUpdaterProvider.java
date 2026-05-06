package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.port.inbox.InboxUpdater;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.InboxUpdaterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(InboxUpdaterProvider.class)
public class FileSystemInboxUpdaterProvider implements InboxUpdaterProvider {
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
    public InboxUpdater create(PluginConfiguration configuration) {
        return new FileSystemInboxUpdater(configuration.require("path"));
    }
}
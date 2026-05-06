package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.core.port.inbox.InboxReader;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(InboxReaderProvider.class)
public class FileSystemInboxReaderProvider implements InboxReaderProvider {
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
    public InboxReader create(PluginConfiguration configuration) {
        return new FileSystemInboxReader(configuration.require("path"));
    }
}
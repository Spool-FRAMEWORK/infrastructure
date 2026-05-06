package software.spool.infrastructure.adapter.inbox.filesystem;

import software.spool.crawler.api.port.InboxWriter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(InboxWriterProvider.class)
public class FileSystemInboxWriterProvider implements InboxWriterProvider {
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
    public InboxWriter create(PluginConfiguration configuration) {
        return new FileSystemInboxWriter(configuration.require("path"));
    }
}
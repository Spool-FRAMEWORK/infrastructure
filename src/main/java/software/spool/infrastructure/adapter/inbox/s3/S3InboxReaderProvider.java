package software.spool.infrastructure.adapter.inbox.s3;

import software.spool.core.port.inbox.InboxReader;
import software.spool.infrastructure.adapter.concurrency.ConcurrencyConfiguration;
import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(InboxReaderProvider.class)
public class S3InboxReaderProvider implements InboxReaderProvider {
    @Override
    public String name() {
        return "S3";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("region") &&
                configuration.has("bucket") &&
                configuration.has("endpoint");
    }

    @Override
    public InboxReader create(PluginConfiguration configuration) {
        return new S3InboxReader(
                S3ClientFactory.create(configuration),
                configuration.require("bucket"),
                ConcurrencyConfiguration.from(configuration));
    }
}

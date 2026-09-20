package software.spool.infrastructure.adapter.inbox.s3;

import software.spool.crawler.api.port.InboxWriter;
import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(InboxWriterProvider.class)
public class S3InboxWriterProvider implements InboxWriterProvider {
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
    public InboxWriter create(PluginConfiguration configuration) {
        return new S3InboxWriter(
                S3ClientFactory.create(configuration),
                configuration.require("bucket"));
    }
}

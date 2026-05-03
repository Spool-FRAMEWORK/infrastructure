package software.spool.infrastructure.adapter.pollsource.http;

import software.spool.crawler.api.port.source.PollSource;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.PollSourceProvider;

@SpoolPlugin(PollSourceProvider.class)
public class HTTPPollSourceProvider implements PollSourceProvider {
    @Override
    public String name() {
        return "HTTP";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("url") && configuration.has("sourceId");
    }

    @Override
    public PollSource<?> create(PluginConfiguration configuration) {
        return new HTTPPollSource(configuration.require("url"), configuration.require("sourceId"));
    }
}

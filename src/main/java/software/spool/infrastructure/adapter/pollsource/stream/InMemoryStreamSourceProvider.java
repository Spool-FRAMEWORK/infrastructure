package software.spool.infrastructure.adapter.pollsource.stream;

import software.spool.core.model.Event;
import software.spool.crawler.api.port.source.StreamSource;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.StreamSourceProvider;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;

@SpoolPlugin(StreamSourceProvider.class)
public class InMemoryStreamSourceProvider implements StreamSourceProvider {
    @Override
    public String name() {
        return "IN_MEMORY";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("eventClassName");
    }

    @SuppressWarnings("unchecked")
    @Override
    public StreamSource<?> create(PluginConfiguration configuration) {
        try {
            return new InMemoryStreamSource<>(
                    PluginResolver.resolve(EventBusProvider.class, configuration),
                    (Class<Event>) Class.forName("events." + configuration.require("eventClassName")),
                    configuration.require("sourceId")
            );
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to create InMemoryStreamSource: evenClassName class not found", e);
        }
    }
}

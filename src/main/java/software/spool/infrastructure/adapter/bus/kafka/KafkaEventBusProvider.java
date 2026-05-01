package software.spool.infrastructure.adapter.bus.kafka;

import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.EventBusProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(EventBusProvider.class)
public class KafkaEventBusProvider implements EventBusProvider {
    @Override
    public String name() {
        return "KAFKA";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("bootstrap.servers");
    }

    @Override
    public EventBus create(PluginConfiguration configuration) {
        return new KafkaEventBus(new KafkaEventBusConfig(
                configuration.require("bootstrap.servers")
        ));
    }
}
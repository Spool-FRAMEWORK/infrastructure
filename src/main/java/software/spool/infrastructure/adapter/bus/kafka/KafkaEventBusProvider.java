package software.spool.infrastructure.adapter.bus.kafka;

import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.spi.PluginRegistry;
import software.spool.infrastructure.spi.provider.EventBusProvider;

public class KafkaEventBusProvider implements EventBusProvider {
    static {
        PluginRegistry.register(EventBusProvider.class, new KafkaEventBusProvider());
    }

    @Override
    public String name() {
        return "KAFKA";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(String url) {
        return url != null;
    }

    @Override
    public EventBus create(String url) {
        return new KafkaEventBus(new KafkaEventBusConfig(url));
    }
}
package software.spool.infrastructure;

import software.spool.infrastructure.bus.kafka.KafkaEventBusProvider;
import software.spool.infrastructure.spi.PluginRegistry;
import software.spool.infrastructure.spi.provider.EventBusProvider;

public final class PluginBootStrap {
    public static void init() {
        PluginRegistry.register(EventBusProvider.class, new KafkaEventBusProvider());
    }
}

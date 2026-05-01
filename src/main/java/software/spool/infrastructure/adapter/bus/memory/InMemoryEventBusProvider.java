package software.spool.infrastructure.adapter.bus.memory;

import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.spi.provider.EventBusProvider;

public class InMemoryEventBusProvider implements EventBusProvider {
    static {
        PluginRegistry.register(EventBusProvider.class, new InMemoryEventBusProvider());
    }

    @Override
    public String name() {
        return "MEMORY";
    }

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public boolean supports(String url) {
        return url != null;
    }

    @Override
    public EventBus create(String url) {
        return new InMemoryEventBus();
    }
}
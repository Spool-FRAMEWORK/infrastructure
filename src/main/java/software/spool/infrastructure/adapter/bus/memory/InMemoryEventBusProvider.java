package software.spool.infrastructure.adapter.bus.memory;

import software.spool.core.adapter.memory.InMemoryEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(EventBusProvider.class)
public class InMemoryEventBusProvider implements EventBusProvider {
    private static final EventBus SHARED_BUS = new InMemoryEventBus();

    @Override
    public String name() {
        return "IN_MEMORY";
    }

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return true;
    }

    @Override
    public EventBus create(PluginConfiguration configuration) {
        return SHARED_BUS;
    }
}
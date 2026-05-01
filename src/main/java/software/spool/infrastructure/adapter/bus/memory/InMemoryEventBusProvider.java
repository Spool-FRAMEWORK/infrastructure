package software.spool.infrastructure.adapter.bus.memory;

import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.EventBusProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

@SpoolPlugin(EventBusProvider.class)
public class InMemoryEventBusProvider implements EventBusProvider {

    @Override
    public String name() {
        return "MEMORY";
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
        return new InMemoryEventBus();
    }
}
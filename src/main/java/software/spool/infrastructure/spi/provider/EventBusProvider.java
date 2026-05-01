package software.spool.infrastructure.spi.provider;

import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.spi.Plugin;

public interface EventBusProvider extends Plugin {
    int priority();
    boolean supports(String url);
    EventBus create(String url);
    default EventBus create() {
        return create(null);
    };
}
package software.spool.infrastructure.adapter.bus.memory;

import software.spool.core.adapter.memory.InMemoryEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.adapter.bus.EventBusDeliveryContract;

import java.time.Duration;

class InMemoryEventBusDeliveryTest extends EventBusDeliveryContract {

    private final EventBus bus = new InMemoryEventBus();

    @Override
    protected EventBus publisherBus() {
        return bus;
    }

    @Override
    protected EventBus newSubscriberBus() {
        return bus;
    }

    @Override
    protected Duration deliveryTimeout() {
        return Duration.ofSeconds(1);
    }
}

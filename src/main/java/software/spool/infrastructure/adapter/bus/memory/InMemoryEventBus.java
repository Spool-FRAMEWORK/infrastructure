package software.spool.infrastructure.adapter.bus.memory;

import software.spool.core.adapter.memory.InMemorySubscription;
import software.spool.core.exception.EventBusEmitException;
import software.spool.core.exception.EventBusSubscriptionException;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.bus.Subscription;
import software.spool.core.utils.routing.DefaultEventRouter;
import software.spool.core.utils.routing.EventRouter;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe in-memory {@link EventBus} implementation for local
 * testing and single-process deployments.
 *
 * <p>
 * Handlers are stored in a {@link ConcurrentHashMap} keyed by destination and
 * dispatched synchronously on the calling thread.
 * </p>
 */
public class InMemoryEventBus implements EventBus {
    private final ConcurrentHashMap<Class<? extends Event>, CopyOnWriteArrayList<Handler<?>>> registry =
            new ConcurrentHashMap<>();
    private final EventRouter router;

    public InMemoryEventBus(EventRouter router) {
        this.router = router;
    }

    public InMemoryEventBus() {
        this(new DefaultEventRouter());
    }

    @Override
    public <E extends Event> Subscription subscribe(
            Class<E> eventType,
            Handler<E> handler
    ) throws EventBusSubscriptionException {
        registry.computeIfAbsent(eventType, ignored -> new CopyOnWriteArrayList<>())
                .add(handler);
        return new InMemorySubscription(
                () -> registry.getOrDefault(eventType, new CopyOnWriteArrayList<>()).remove(handler)
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Event> void publish(E event) throws EventBusEmitException {
        try {
            List<Handler<?>> handlers = registry.getOrDefault(
                    event.getClass(),
                    new CopyOnWriteArrayList<>()
            );
            for (Handler<?> handler : handlers) {
                ((Handler<E>) handler).handle(event);
            }
        } catch (Exception e) {
            throw new EventBusEmitException(
                    event,
                    "Failed to publish event to destination [" + router.resolve(event.getClass()) + "]",
                    e
            );
        }
    }
}
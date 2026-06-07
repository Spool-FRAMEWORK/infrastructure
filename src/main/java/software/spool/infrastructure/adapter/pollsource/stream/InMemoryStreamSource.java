package software.spool.infrastructure.adapter.pollsource.stream;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Subscription;
import software.spool.crawler.api.port.source.StreamSource;

import java.util.function.Consumer;

public class InMemoryStreamSource<E extends Event> implements StreamSource<E> {
    private final EventBus bus;
    private final Class<E> eventClass;
    private final String sourceId;
    private Subscription subscription;

    public InMemoryStreamSource(EventBus bus, Class<E> eventClass, String sourceId) {
        this.bus = bus;
        this.eventClass = eventClass;
        this.sourceId = sourceId;
        this.subscription = Subscription.NULL;
    }

    @Override
    public void start(Consumer<E> onMessage, Consumer<Exception> onError) throws SpoolException {
        if (subscription.isActive()) return;
        this.subscription = bus.subscribe(eventClass, e -> {
            try {
                onMessage.accept(e);
            } catch (Exception error) {
                onError.accept(error);
            }
        });
    }

    @Override
    public void stop() {
        this.subscription = Subscription.NULL;
        this.subscription.cancel();
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public E fetch() throws SpoolException {
        return null;
    }
}

package software.spool.infrastructure.adapter.bus.kafka;

import software.spool.core.exception.EventBusEmitException;
import software.spool.core.exception.EventBusSubscriptionException;
import software.spool.core.model.Event;
import software.spool.core.port.bus.*;

public class KafkaEventBus implements EventBus {
    private final EventPublisher publisher;
    private final EventSubscriber subscriber;

    public KafkaEventBus(KafkaEventBusConfig config) {
        this.publisher = new KafkaEventPublisher(config);
        this.subscriber = new KafkaEventSubscriber(config);
    }

    @Override
    public <E extends Event> void publish(E event) throws EventBusEmitException {
        publisher.publish(event);
    }

    @Override
    public <E extends Event> Subscription subscribe(Class<E> eventType, Handler<E> handler) throws EventBusSubscriptionException {
        return subscriber.subscribe(eventType, handler);
    }
}

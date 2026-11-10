package software.spool.infrastructure.adapter.bus.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.EventBusSubscriptionException;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.bus.Subscription;
import software.spool.core.utils.routing.AnnotationEventRouter;
import software.spool.core.utils.routing.EventRoute;
import software.spool.core.utils.routing.EventRouter;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaEventSubscriber implements EventSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger("KafkaEventSubscriber");
    private static final String SINGLE_SUBSCRIBER_OFFSET_RESET = "earliest";
    private static final String EVERY_SUBSCRIBER_OFFSET_RESET = "latest";

    private final Properties baseProps;
    private final EventRouter router;
    private final String subscriberId;

    public KafkaEventSubscriber(KafkaEventBusConfig config, EventRouter router) {
        this.baseProps = buildBaseProps(config);
        this.router = router;
        this.subscriberId = config.subscriberId().orElseGet(KafkaEventSubscriber::temporarySubscriberId);
    }

    public KafkaEventSubscriber(KafkaEventBusConfig config) {
        this(config, new AnnotationEventRouter());
    }



    @Override
    public <E extends Event> Subscription subscribe(
            Class<E> eventType,
            Handler<E> handler
    ) throws EventBusSubscriptionException {
        Properties props = new Properties();
        props.putAll(baseProps);
        EventRoute route = router.routeOf(eventType);
        String channel = route.channel().value();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupOf(route));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetOf(route));

        try {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(List.of(channel));

            KafkaSubscription<E> subscription =
                    new KafkaSubscription<>(consumer, handler, eventType);

            subscription.start();
            return subscription;

        } catch (Exception e) {
            throw new EventBusSubscriptionException(
                    eventType,
                    e.getMessage(),
                    e
            );
        }
    }

    private String consumerGroupOf(EventRoute route) {
        String channel = route.channel().value();
        return switch (route.deliveryPolicy()) {
            case SINGLE_SUBSCRIBER -> channel;
            case EVERY_SUBSCRIBER -> channel + "." + subscriberId;
        };
    }

    private static String offsetResetOf(EventRoute route) {
        return switch (route.deliveryPolicy()) {
            case SINGLE_SUBSCRIBER -> SINGLE_SUBSCRIBER_OFFSET_RESET;
            case EVERY_SUBSCRIBER -> EVERY_SUBSCRIBER_OFFSET_RESET;
        };
    }

    private static String temporarySubscriberId() {
        String subscriberId = UUID.randomUUID().toString();
        LOG.warn("No Kafka subscriber id is configured. Using {} for this run: events delivered to every subscriber "
                + "that are published while this replica is stopped will not reach it. Set subscriber.id to a stable "
                + "value per replica.", subscriberId);
        return subscriberId;
    }

    private static Properties buildBaseProps(KafkaEventBusConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return props;
    }
}
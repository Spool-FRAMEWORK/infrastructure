package software.spool.infrastructure.adapter.bus.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import software.spool.core.exception.EventBusSubscriptionException;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.bus.Subscription;
import software.spool.core.utils.routing.DefaultEventRouter;
import software.spool.core.utils.routing.EventRouter;
import software.spool.core.utils.routing.EventUtils;

import java.util.List;
import java.util.Properties;

public class KafkaEventSubscriber implements EventSubscriber {
    private final Properties baseProps;
    private final EventRouter router;

    public KafkaEventSubscriber(KafkaEventBusConfig config, EventRouter router) {
        this.baseProps = buildBaseProps(config);
        this.router = router;
    }

    public KafkaEventSubscriber(KafkaEventBusConfig config) {
        this(config, new DefaultEventRouter());
    }



    @Override
    public <E extends Event> Subscription subscribe(
            Class<E> eventType,
            Handler<E> handler
    ) throws EventBusSubscriptionException {
        Properties props = new Properties();
        props.putAll(baseProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, EventUtils.resolveAddress(eventType));

        try {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(List.of(router.resolve(eventType)));

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

    private static Properties buildBaseProps(KafkaEventBusConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return props;
    }
}
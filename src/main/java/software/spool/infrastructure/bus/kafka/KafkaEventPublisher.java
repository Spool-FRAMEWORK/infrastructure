package software.spool.infrastructure.bus.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.EventBusEmitException;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.DefaultEventRouter;
import software.spool.core.utils.routing.EventRouter;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaEventPublisher implements EventPublisher, AutoCloseable {
    private final KafkaProducer<String, byte[]> producer;
    private final EventRouter router;

    public KafkaEventPublisher(KafkaEventBusConfig config, EventRouter router) {
        this.producer = new KafkaProducer<>(buildProducerProps(config));
        this.router = router;
    }

    public KafkaEventPublisher(KafkaEventBusConfig config) {
        this(config, new DefaultEventRouter());
    }

    @Override
    public <E extends Event> void publish(E event) throws EventBusEmitException {
        try {
            byte[] payload = RecordSerializerFactory.record()
                    .serialize(event)
                    .getBytes(StandardCharsets.UTF_8);

            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(router.resolve(event.getClass()), payload);

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    throw new EventBusEmitException(
                            event,
                            "Failed to deliver message to destination " + router.resolve(event.getClass()),
                            ex
                    );
                }
            });
        } catch (Exception e) {
            throw new EventBusEmitException(
                    event,
                    "Failed to emit message to Kafka destination [" + router.resolve(event.getClass()) + "]",
                    e
            );
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }

    private static Properties buildProducerProps(KafkaEventBusConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 200);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
        return props;
    }
}
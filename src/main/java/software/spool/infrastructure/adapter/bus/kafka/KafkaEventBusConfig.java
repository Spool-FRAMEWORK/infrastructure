package software.spool.infrastructure.adapter.bus.kafka;

import java.util.Objects;
import java.util.Optional;

/**
 * Connection to Kafka and identity of this subscriber.
 *
 * @param bootstrapServers the Kafka servers to connect to
 * @param subscriberId     a stable id per replica. Events delivered to every subscriber are read by a consumer
 *                         group named after it, so a restarted replica goes on where it stopped. Without it, each
 *                         run uses a new group and misses what was published while it was stopped.
 */
public record KafkaEventBusConfig(String bootstrapServers, Optional<String> subscriberId) {

    public KafkaEventBusConfig {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers is required");
        Objects.requireNonNull(subscriberId, "subscriberId must be present or empty, never null");
    }

    public KafkaEventBusConfig(String bootstrapServers) {
        this(bootstrapServers, Optional.empty());
    }

    public static KafkaEventBusConfig local() {
        return new KafkaEventBusConfig("localhost:9092");
    }
}

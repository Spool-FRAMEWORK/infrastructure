package software.spool.infrastructure.bus.kafka;

public record KafkaEventBusConfig(String bootstrapServers) {
    public static KafkaEventBusConfig local() {
        return new KafkaEventBusConfig("localhost:9092");
    }
}

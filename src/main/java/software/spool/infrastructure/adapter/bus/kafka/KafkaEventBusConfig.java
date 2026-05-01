package software.spool.infrastructure.adapter.bus.kafka;

public record KafkaEventBusConfig(String bootstrapServers) {
    public static KafkaEventBusConfig local() {
        return new KafkaEventBusConfig("localhost:9092");
    }
}

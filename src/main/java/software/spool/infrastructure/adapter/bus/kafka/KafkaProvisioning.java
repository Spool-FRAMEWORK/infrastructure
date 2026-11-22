package software.spool.infrastructure.adapter.bus.kafka;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.provisioning.ProvisioningTarget;
import software.spool.infrastructure.provisioning.TerraformFragment;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Terraform of a Kafka bus: locally, a single broker in a container, announced on the port of the first of its
 * bootstrap servers. In AWS it cannot be generated yet: that would be MSK, a decision of its own.
 */
final class KafkaProvisioning implements PieceProvisioning {
    static final String KAFKA_IMAGE = "apache/kafka:3.8.0";
    static final String KAFKA_CONTAINER = "spool-kafka";
    private static final int KAFKA_PORT = 9092;

    private final String bootstrapServers;

    KafkaProvisioning(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Optional<TerraformFragment> forTarget(ProvisioningTarget target) {
        if (target != ProvisioningTarget.LOCAL) return Optional.empty();
        int port = portOf(bootstrapServers);
        return Optional.of(TerraformFragment.builder()
                .requiredProvider("docker", "kreuzwerker/docker", "~> 3.0")
                .variable("kafka_image", Map.of("type", "string", "default", KAFKA_IMAGE))
                .variable("kafka_container_name", Map.of("type", "string", "default", KAFKA_CONTAINER))
                .resource("docker_image", "kafka", Map.of("name", "${var.kafka_image}", "keep_locally", true))
                .resource("docker_container", "kafka", Map.of(
                        "name", "${var.kafka_container_name}",
                        "image", "${docker_image.kafka.image_id}",
                        "ports", List.of(Map.of("internal", KAFKA_PORT, "external", port)),
                        "env", List.of(
                                "KAFKA_NODE_ID=1",
                                "KAFKA_PROCESS_ROLES=broker,controller",
                                "KAFKA_LISTENERS=PLAINTEXT://:" + KAFKA_PORT + ",CONTROLLER://:9093",
                                "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:" + port,
                                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1")))
                .build());
    }

    /** The port of the first bootstrap server, or Kafka's own when it does not say it or is a variable. */
    private static int portOf(String bootstrapServers) {
        String first = bootstrapServers.split(",")[0].strip();
        int colon = first.lastIndexOf(':');
        if (colon < 0) return KAFKA_PORT;
        try {
            return Integer.parseInt(first.substring(colon + 1));
        } catch (NumberFormatException notAPort) {
            return KAFKA_PORT;
        }
    }
}

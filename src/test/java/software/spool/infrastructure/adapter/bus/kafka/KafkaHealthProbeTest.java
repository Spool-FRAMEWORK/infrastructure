package software.spool.infrastructure.adapter.bus.kafka;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthStatus;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaHealthProbeTest {
    private static final String TEST_BROKER = "localhost:9092";

    @Test
    void probe_brokersThatDoNotAnswer_isUnhealthyAndNamesThem() throws Exception {
        String nobody = "localhost:" + freePort();

        HealthCheck check = new KafkaHealthProbe(nobody, Duration.ofMillis(500)).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.reason()).contains(nobody);
    }

    @Test
    void provider_supportsAPieceWithItsBootstrapServers() {
        KafkaHealthProbeProvider provider = new KafkaHealthProbeProvider();

        assertThat(provider.name()).isEqualTo("KAFKA");
        assertThat(provider.supports(PluginConfiguration.builder().with("bootstrap.servers", TEST_BROKER).build())).isTrue();
        assertThat(provider.supports(PluginConfiguration.builder().build())).isFalse();
    }

    @Test
    @Tag("docker")
    void probe_brokerThatAnswers_isHealthy() {
        Assumptions.assumeTrue(isReachable(), "Kafka not available on " + TEST_BROKER);

        assertThat(new KafkaHealthProbe(TEST_BROKER, Duration.ofSeconds(5)).probe().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static boolean isReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 9092), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

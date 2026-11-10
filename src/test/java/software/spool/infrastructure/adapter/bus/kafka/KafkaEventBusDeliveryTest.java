package software.spool.infrastructure.adapter.bus.kafka;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.adapter.bus.EventBusDeliveryContract;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class KafkaEventBusDeliveryTest extends EventBusDeliveryContract {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private final EventBus publisher = new KafkaEventBus(new KafkaEventBusConfig(BOOTSTRAP_SERVERS));

    @BeforeAll
    static void assumeKafkaIsRunning() {
        Assumptions.assumeTrue(isReachable(),
                "Kafka not available on " + BOOTSTRAP_SERVERS + ": run docker compose -f .docker/docker-compose.yml up -d kafka");
    }

    @Override
    protected EventBus publisherBus() {
        return publisher;
    }

    @Override
    protected EventBus newSubscriberBus() {
        return new KafkaEventBus(new KafkaEventBusConfig(BOOTSTRAP_SERVERS));
    }

    @Override
    protected Duration deliveryTimeout() {
        return Duration.ofSeconds(30);
    }

    @Override
    protected Duration settleTime() {
        return Duration.ofSeconds(2);
    }

    @Test
    void publish_envelopePersistedWithJanitorAndMounterSubscribed_reachesBoth() throws Exception {
        List<String> janitor = subscribe(EnvelopePersisted.class, event -> event.idempotencyKey().value());
        List<String> mounter = subscribe(EnvelopePersisted.class, event -> event.idempotencyKey().value());
        Duration timeout = deliveryTimeout();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int published = 0;
        while ((janitor.isEmpty() || mounter.isEmpty()) && System.currentTimeMillis() < deadline) {
            publisher.publish(envelopePersisted("warm-up-" + published++));
            Thread.sleep(200);
        }
        janitor.clear();
        mounter.clear();

        for (int index = 0; index < 10; index++) {
            publisher.publish(envelopePersisted("persisted-" + index));
        }

        long secondDeadline = System.currentTimeMillis() + timeout.toMillis();
        while ((countPersisted(janitor) < 10 || countPersisted(mounter) < 10) && System.currentTimeMillis() < secondDeadline) {
            Thread.sleep(100);
        }
        assertThat(countPersisted(janitor)).isEqualTo(10);
        assertThat(countPersisted(mounter)).isEqualTo(10);
    }

    private static EnvelopePersisted envelopePersisted(String key) {
        return EnvelopePersisted.builder()
                .idempotencyKey(new IdempotencyKey(key))
                .partitionKey(new PartitionKey("source=orders"))
                .build();
    }

    private static long countPersisted(List<String> received) {
        return received.stream().filter(key -> key.startsWith("persisted-")).distinct().count();
    }

    private static boolean isReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 9092), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}

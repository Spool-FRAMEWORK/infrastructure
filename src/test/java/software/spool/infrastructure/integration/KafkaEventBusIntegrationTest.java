package software.spool.infrastructure.integration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import software.spool.core.model.Event;
import software.spool.core.port.bus.Subscription;
import software.spool.infrastructure.adapter.bus.kafka.KafkaEventBus;
import software.spool.infrastructure.adapter.bus.kafka.KafkaEventBusConfig;
import software.spool.infrastructure.adapter.bus.kafka.KafkaEventBusProvider;
import software.spool.infrastructure.fixture.PluginConfigurationMother;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class KafkaEventBusIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @BeforeAll
    static void assumeKafkaIsRunning() {
        Assumptions.assumeTrue(isReachable("localhost", 9092),
                "Kafka not available on localhost:9092 — run: docker compose -f .docker/docker-compose.yml up -d");
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record DeliveryEvent(String payload) implements Event {
        @Override public String eventId() { return payload; }
        @Override public String causationId() { return ""; }
        @Override public String correlationId() { return ""; }
        @Override public Instant timestamp() { return Instant.EPOCH; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record MultipleEvent(String payload) implements Event {
        @Override public String eventId() { return payload; }
        @Override public String causationId() { return ""; }
        @Override public String correlationId() { return ""; }
        @Override public Instant timestamp() { return Instant.EPOCH; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record RoutingEventA(String payload) implements Event {
        @Override public String eventId() { return payload; }
        @Override public String causationId() { return ""; }
        @Override public String correlationId() { return ""; }
        @Override public Instant timestamp() { return Instant.EPOCH; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record RoutingEventB(String payload) implements Event {
        @Override public String eventId() { return payload; }
        @Override public String causationId() { return ""; }
        @Override public String correlationId() { return ""; }
        @Override public Instant timestamp() { return Instant.EPOCH; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record CancelEvent(String payload) implements Event {
        @Override public String eventId() { return payload; }
        @Override public String causationId() { return ""; }
        @Override public String correlationId() { return ""; }
        @Override public Instant timestamp() { return Instant.EPOCH; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ProviderEvent(String payload) implements Event {
        @Override public String eventId() { return payload; }
        @Override public String causationId() { return ""; }
        @Override public String correlationId() { return ""; }
        @Override public Instant timestamp() { return Instant.EPOCH; }
    }

    private final List<Subscription> openSubscriptions = new ArrayList<>();

    @AfterEach
    void cancelSubscriptions() {
        openSubscriptions.forEach(Subscription::cancel);
        openSubscriptions.clear();
    }

    private KafkaEventBus createBus() {
        return new KafkaEventBus(new KafkaEventBusConfig(BOOTSTRAP));
    }

    @Test
    void publish_singleSubscriber_handlerReceivesEvent() throws Exception {
        KafkaEventBus bus = createBus();
        CountDownLatch latch = new CountDownLatch(1);
        List<DeliveryEvent> received = new CopyOnWriteArrayList<>();

        openSubscriptions.add(bus.subscribe(DeliveryEvent.class, event -> {
            received.add(event);
            latch.countDown();
        }));

        bus.publish(new DeliveryEvent("hello-kafka"));

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received).hasSize(1);
        assertThat(received.get(0).payload()).isEqualTo("hello-kafka");
    }

    @Test
    void publish_multipleEvents_subscriberReceivesAll() throws Exception {
        KafkaEventBus bus = createBus();
        CountDownLatch latch = new CountDownLatch(3);
        List<String> payloads = new CopyOnWriteArrayList<>();

        openSubscriptions.add(bus.subscribe(MultipleEvent.class, event -> {
            payloads.add(event.payload());
            latch.countDown();
        }));

        bus.publish(new MultipleEvent("one"));
        bus.publish(new MultipleEvent("two"));
        bus.publish(new MultipleEvent("three"));

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(payloads).containsExactlyInAnyOrder("one", "two", "three");
    }

    @Test
    void publish_differentEventTypes_subscriberOnlyReceivesOwnType() throws Exception {
        KafkaEventBus bus = createBus();
        CountDownLatch latchA = new CountDownLatch(1);
        List<RoutingEventB> receivedByB = new CopyOnWriteArrayList<>();

        openSubscriptions.add(bus.subscribe(RoutingEventA.class, e -> latchA.countDown()));
        openSubscriptions.add(bus.subscribe(RoutingEventB.class, receivedByB::add));

        bus.publish(new RoutingEventA("only-for-A"));

        assertThat(latchA.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(receivedByB).isEmpty();
    }

    @Test
    void subscription_cancel_subscriptionBecomesInactive() throws Exception {
        KafkaEventBus bus = createBus();
        CountDownLatch latch = new CountDownLatch(1);

        Subscription sub = bus.subscribe(CancelEvent.class, e -> latch.countDown());
        openSubscriptions.add(sub);

        assertThat(sub.isActive()).isTrue();

        bus.publish(new CancelEvent("trigger"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        sub.cancel();

        assertThat(sub.isActive()).isFalse();
    }

    @Test
    void provider_create_withRealBootstrapServers_producesWorkingBus() throws Exception {
        var config = PluginConfigurationMother.withBootstrapServers(BOOTSTRAP);
        var bus = (KafkaEventBus) new KafkaEventBusProvider().create(config);

        CountDownLatch latch = new CountDownLatch(1);
        List<ProviderEvent> received = new CopyOnWriteArrayList<>();

        openSubscriptions.add(bus.subscribe(ProviderEvent.class, event -> {
            received.add(event);
            latch.countDown();
        }));

        bus.publish(new ProviderEvent("from-provider"));

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received.get(0).payload()).isEqualTo("from-provider");
    }

    private static boolean isReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

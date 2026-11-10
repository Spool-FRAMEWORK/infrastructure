package software.spool.infrastructure.adapter.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Subscription;
import software.spool.infrastructure.fixture.OrderPlaced;
import software.spool.infrastructure.fixture.PriceChanged;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Delivery rules every {@link EventBus} adapter must follow. Each subscriber bus stands for a different
 * module or replica: for an in-process bus they are the same instance, for a distributed one they are not.
 */
public abstract class EventBusDeliveryContract {

    private final List<Subscription> subscriptions = new ArrayList<>();

    protected abstract EventBus publisherBus();

    protected abstract EventBus newSubscriberBus();

    protected abstract Duration deliveryTimeout();

    @AfterEach
    void cancelSubscriptions() {
        subscriptions.forEach(Subscription::cancel);
        subscriptions.clear();
    }

    @Test
    void publish_eventWithoutDeliveryPolicyAndTwoSubscribers_reachesOnlyOneOfThem() throws Exception {
        List<String> first = subscribe(OrderPlaced.class, OrderPlaced::payload);
        List<String> second = subscribe(OrderPlaced.class, OrderPlaced::payload);
        awaitSubscribersReady(OrderPlaced::new, () -> !first.isEmpty() || !second.isEmpty(), first, second);

        String payload = uniquePayload();
        publisherBus().publish(new OrderPlaced(payload));

        waitUntil(() -> first.contains(payload) || second.contains(payload));
        Thread.sleep(settleTime().toMillis());
        assertThat(countOf(payload, first) + countOf(payload, second)).isEqualTo(1);
    }

    @Test
    void publish_eventDeliveredToEverySubscriberAndTwoSubscribers_reachesBoth() throws Exception {
        List<String> first = subscribe(PriceChanged.class, PriceChanged::payload);
        List<String> second = subscribe(PriceChanged.class, PriceChanged::payload);
        awaitSubscribersReady(PriceChanged::new, () -> !first.isEmpty() && !second.isEmpty(), first, second);

        String payload = uniquePayload();
        publisherBus().publish(new PriceChanged(payload));

        waitUntil(() -> first.contains(payload) && second.contains(payload));
        assertThat(first).contains(payload);
        assertThat(second).contains(payload);
    }

    protected Duration settleTime() {
        return Duration.ZERO;
    }

    protected <E extends Event> List<String> subscribe(Class<E> eventType, Function<E, String> payloadOf) throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        subscriptions.add(newSubscriberBus().subscribe(eventType, event -> received.add(payloadOf.apply(event))));
        return received;
    }

    private void awaitSubscribersReady(Function<String, Event> warmUpEvent, BooleanSupplier ready,
                                       List<String> first, List<String> second) throws Exception {
        Instant deadline = Instant.now().plus(deliveryTimeout());
        while (!ready.getAsBoolean() && Instant.now().isBefore(deadline)) {
            publisherBus().publish(warmUpEvent.apply("warm-up-" + UUID.randomUUID()));
            Thread.sleep(200);
        }
        assertThat(ready.getAsBoolean()).as("subscribers ready before the timeout").isTrue();
        Thread.sleep(settleTime().toMillis());
        first.clear();
        second.clear();
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(deliveryTimeout());
        while (!condition.getAsBoolean() && Instant.now().isBefore(deadline)) {
            Thread.sleep(50);
        }
    }

    private static long countOf(String payload, List<String> received) {
        return received.stream().filter(payload::equals).count();
    }

    private static String uniquePayload() {
        return "event-" + UUID.randomUUID();
    }
}

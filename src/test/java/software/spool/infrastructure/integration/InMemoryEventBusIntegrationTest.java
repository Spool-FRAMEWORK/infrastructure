package software.spool.infrastructure.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.spool.core.exception.EventBusEmitException;
import software.spool.core.port.bus.Subscription;
import software.spool.infrastructure.adapter.bus.memory.InMemoryEventBus;
import software.spool.infrastructure.adapter.bus.memory.InMemoryEventBusProvider;
import software.spool.infrastructure.fixture.AnotherTestEvent;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.fixture.TestEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryEventBusIntegrationTest {

    private InMemoryEventBus bus;

    @BeforeEach
    void setUp() {
        bus = new InMemoryEventBus();
    }

    @Test
    void provider_create_returnsFunctionalEventBus() {
        assertThat(new InMemoryEventBusProvider().create(PluginConfigurationMother.empty())).isNotNull();
    }

    @Test
    void fullLifecycle_publishAfterSubscribe_handlerReceivesAllEvents() throws Exception {
        List<String> received = new ArrayList<>();
        bus.subscribe(TestEvent.class, e -> received.add(e.payload()));

        bus.publish(new TestEvent("one"));
        bus.publish(new TestEvent("two"));
        bus.publish(new TestEvent("three"));

        assertThat(received).containsExactly("one", "two", "three");
    }

    @Test
    void fullLifecycle_multipleEventTypes_routedIndependently() throws Exception {
        List<String> testEvents = new ArrayList<>();
        List<Integer> otherEvents = new ArrayList<>();

        bus.subscribe(TestEvent.class, e -> testEvents.add(e.payload()));
        bus.subscribe(AnotherTestEvent.class, e -> otherEvents.add(e.value()));

        bus.publish(new TestEvent("A"));
        bus.publish(new AnotherTestEvent(1));
        bus.publish(new TestEvent("B"));
        bus.publish(new AnotherTestEvent(2));

        assertThat(testEvents).containsExactly("A", "B");
        assertThat(otherEvents).containsExactly(1, 2);
    }

    @Test
    void subscription_cancel_stopsFurtherDelivery() throws Exception {
        List<String> received = new ArrayList<>();
        Subscription sub = bus.subscribe(TestEvent.class, e -> received.add(e.payload()));

        bus.publish(new TestEvent("before"));
        sub.cancel();
        bus.publish(new TestEvent("after"));

        assertThat(received).containsExactly("before");
    }

    @Test
    void subscription_cancelOneOfMany_othersContinueReceiving() throws Exception {
        List<String> log = new ArrayList<>();
        Subscription first = bus.subscribe(TestEvent.class, e -> log.add("first"));
        Subscription second = bus.subscribe(TestEvent.class, e -> log.add("second"));

        bus.publish(new TestEvent("x"));
        first.cancel();
        bus.publish(new TestEvent("y"));
        second.cancel();
        bus.publish(new TestEvent("z"));

        assertThat(log).containsExactly("first", "second", "second");
    }

    @Test
    void subscription_isActive_falseAfterCancel() throws Exception {
        Subscription sub = bus.subscribe(TestEvent.class, e -> {});
        assertThat(sub.isActive()).isTrue();

        sub.cancel();
        assertThat(sub.isActive()).isFalse();
    }

    @Test
    void publish_handlerThrows_wrapsAsEventBusEmitException() throws Exception {
        bus.subscribe(TestEvent.class, e -> { throw new RuntimeException("handler-failure"); });

        assertThatThrownBy(() -> bus.publish(new TestEvent("x")))
                .isInstanceOf(EventBusEmitException.class)
                .hasMessageContaining("handler-failure");
    }

    @Test
    void publish_firstHandlerThrows_remainingHandlersSkipped() throws Exception {
        List<String> log = new ArrayList<>();
        bus.subscribe(TestEvent.class, e -> { throw new RuntimeException("first throws"); });
        bus.subscribe(TestEvent.class, e -> log.add("second"));

        assertThatThrownBy(() -> bus.publish(new TestEvent("x")))
                .isInstanceOf(EventBusEmitException.class);

        assertThat(log).isEmpty();
    }

    @Test
    void publish_concurrentPublishers_allEventsDelivered() throws Exception {
        int threadCount = 10;
        int eventsPerThread = 50;
        List<TestEvent> received = new CopyOnWriteArrayList<>();
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);

        bus.subscribe(TestEvent.class, received::add);

        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            String tName = "t" + t;
            exec.submit(() -> {
                try {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < eventsPerThread; i++) {
                        bus.publish(new TestEvent(tName + "-" + i));
                    }
                } catch (Exception ignored) {}
            });
        }

        ready.await();
        start.countDown();
        exec.shutdown();
        boolean finished = exec.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(finished).isTrue();
        assertThat(received).hasSize(threadCount * eventsPerThread);
    }
}

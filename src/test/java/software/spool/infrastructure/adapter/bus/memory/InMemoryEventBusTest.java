package software.spool.infrastructure.adapter.bus.memory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.spool.core.exception.EventBusEmitException;
import software.spool.core.port.bus.Subscription;
import software.spool.infrastructure.fixture.AnotherTestEvent;
import software.spool.infrastructure.fixture.TestEvent;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryEventBusTest {

    private InMemoryEventBus bus;

    @BeforeEach
    void setUp() {
        bus = new InMemoryEventBus();
    }

    @Test
    void publish_singleSubscriber_handlerReceivesEvent() throws Exception {
        List<TestEvent> received = new ArrayList<>();
        bus.subscribe(TestEvent.class, received::add);

        bus.publish(new TestEvent("hello"));

        assertThat(received).hasSize(1);
        assertThat(received.get(0).payload()).isEqualTo("hello");
    }

    @Test
    void publish_multipleSubscribers_allHandlersReceiveEvent() throws Exception {
        List<String> log = new ArrayList<>();
        bus.subscribe(TestEvent.class, e -> log.add("first:" + e.payload()));
        bus.subscribe(TestEvent.class, e -> log.add("second:" + e.payload()));

        bus.publish(new TestEvent("ping"));

        assertThat(log).containsExactlyInAnyOrder("first:ping", "second:ping");
    }

    @Test
    void publish_noSubscribers_doesNotThrow() throws Exception {
        bus.publish(new AnotherTestEvent(42));
    }

    @Test
    void publish_handlerRegisteredForDifferentEventType_doesNotReceive() throws Exception {
        List<AnotherTestEvent> received = new ArrayList<>();
        bus.subscribe(AnotherTestEvent.class, received::add);

        bus.publish(new TestEvent("irrelevant"));

        assertThat(received).isEmpty();
    }

    @Test
    void publish_eventPublishedToCorrectTypeOnly() throws Exception {
        List<TestEvent> testEvents = new ArrayList<>();
        List<AnotherTestEvent> otherEvents = new ArrayList<>();

        bus.subscribe(TestEvent.class, testEvents::add);
        bus.subscribe(AnotherTestEvent.class, otherEvents::add);

        bus.publish(new TestEvent("t"));
        bus.publish(new AnotherTestEvent(1));

        assertThat(testEvents).hasSize(1);
        assertThat(otherEvents).hasSize(1);
    }

    @Test
    void subscribe_returnsActiveSubscription() throws Exception {
        Subscription sub = bus.subscribe(TestEvent.class, e -> {});
        assertThat(sub.isActive()).isTrue();
    }

    @Test
    void cancel_handlerNoLongerReceivesEvents() throws Exception {
        List<TestEvent> received = new ArrayList<>();
        Subscription sub = bus.subscribe(TestEvent.class, received::add);

        sub.cancel();
        bus.publish(new TestEvent("after-cancel"));

        assertThat(received).isEmpty();
    }

    @Test
    void cancel_onlyRemovesTargetHandler_otherHandlersStillReceive() throws Exception {
        List<String> log = new ArrayList<>();
        Subscription first = bus.subscribe(TestEvent.class, e -> log.add("first"));
        bus.subscribe(TestEvent.class, e -> log.add("second"));

        first.cancel();
        bus.publish(new TestEvent("x"));

        assertThat(log).containsExactly("second");
    }

    @Test
    void publish_handlerThrowsRuntimeException_wrapsInEventBusEmitException() throws Exception {
        bus.subscribe(TestEvent.class, e -> { throw new IllegalStateException("boom"); });

        assertThatThrownBy(() -> bus.publish(new TestEvent("trigger")))
                .isInstanceOf(EventBusEmitException.class)
                .hasMessageContaining("boom");
    }

    @Test
    void instance_calledMultipleTimes_returnsSameObject() {
        assertThat(InMemoryEventBus.instance()).isSameAs(InMemoryEventBus.instance());
    }

    @Test
    void constructor_withCustomRouter_createsIndependentBus() throws Exception {
        InMemoryEventBus busA = new InMemoryEventBus();
        InMemoryEventBus busB = new InMemoryEventBus();

        List<TestEvent> received = new ArrayList<>();
        busA.subscribe(TestEvent.class, received::add);

        busB.publish(new TestEvent("cross-bus"));

        assertThat(received).isEmpty();
    }
}

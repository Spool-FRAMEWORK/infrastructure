package software.spool.infrastructure.adapter.pollsource.stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Subscription;
import software.spool.infrastructure.adapter.bus.memory.InMemoryEventBus;
import software.spool.infrastructure.fixture.TestEvent;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InMemoryStreamSourceTest {

    @Mock private EventBus bus;
    @Mock private Subscription subscription;

    @Test
    void sourceId_returnsConfiguredValue() {
        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(bus, TestEvent.class, "my-source");
        assertThat(source.sourceId()).isEqualTo("my-source");
    }

    @Test
    void fetch_alwaysReturnsNull() {
        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(bus, TestEvent.class, "src");
        assertThat(source.fetch()).isNull();
    }

    @Test
    void start_delegatesSubscribeToBus() throws Exception {
        when(bus.subscribe(eq(TestEvent.class), any())).thenReturn(subscription);

        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(bus, TestEvent.class, "src");
        source.start(e -> {}, ex -> {});

        verify(bus, times(1)).subscribe(eq(TestEvent.class), any());
    }

    @Test
    void start_whenAlreadyActive_doesNotResubscribe() throws Exception {
        when(bus.subscribe(eq(TestEvent.class), any())).thenReturn(subscription);
        when(subscription.isActive()).thenReturn(true);

        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(bus, TestEvent.class, "src");
        source.start(e -> {}, ex -> {});
        source.start(e -> {}, ex -> {});

        verify(bus, times(1)).subscribe(any(), any());
    }

    @Test
    void stop_resetsSubscription_allowsRestart() throws Exception {
        when(bus.subscribe(eq(TestEvent.class), any())).thenReturn(subscription);

        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(bus, TestEvent.class, "src");
        source.start(e -> {}, ex -> {});
        source.stop();
        source.start(e -> {}, ex -> {});

        verify(bus, times(2)).subscribe(eq(TestEvent.class), any());
    }

    @Test
    void start_onMessage_forwardsEventsToConsumer() throws Exception {
        InMemoryEventBus realBus = new InMemoryEventBus();
        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(realBus, TestEvent.class, "src");
        List<TestEvent> received = new ArrayList<>();

        source.start(received::add, ex -> {});
        realBus.publish(new TestEvent("hello"));
        realBus.publish(new TestEvent("world"));

        assertThat(received).extracting(TestEvent::payload).containsExactly("hello", "world");
    }

    @Test
    void start_whenHandlerThrows_forwardsExceptionToErrorConsumer() throws Exception {
        InMemoryEventBus realBus = new InMemoryEventBus();
        InMemoryStreamSource<TestEvent> source = new InMemoryStreamSource<>(realBus, TestEvent.class, "src");
        List<Exception> errors = new ArrayList<>();
        RuntimeException handlerError = new RuntimeException("oops");

        source.start(e -> { throw handlerError; }, errors::add);

        realBus.publish(new TestEvent("trigger"));

        assertThat(errors).hasSize(1).first().isSameAs(handlerError);
    }
}

package software.spool.infrastructure.fixture;

import software.spool.core.model.Event;

import java.time.Instant;

public record TestEvent(String payload) implements Event {
    @Override public String eventId() { return "test-event-id"; }
    @Override public String causationId() { return "test-causation-id"; }
    @Override public String correlationId() { return "test-correlation-id"; }
    @Override public Instant timestamp() { return Instant.EPOCH; }
}

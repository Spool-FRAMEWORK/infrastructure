package software.spool.infrastructure.fixture;

import software.spool.core.model.Event;

import java.time.Instant;

public record AnotherTestEvent(int value) implements Event {
    @Override public String eventId() { return "another-event-id"; }
    @Override public String causationId() { return "another-causation-id"; }
    @Override public String correlationId() { return "another-correlation-id"; }
    @Override public Instant timestamp() { return Instant.EPOCH; }
}

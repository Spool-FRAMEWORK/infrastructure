package software.spool.infrastructure.fixture;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import software.spool.core.model.Event;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderPlaced(String payload) implements Event {
    @Override public String eventId() { return payload; }
    @Override public String causationId() { return ""; }
    @Override public String correlationId() { return ""; }
    @Override public Instant timestamp() { return Instant.EPOCH; }
}

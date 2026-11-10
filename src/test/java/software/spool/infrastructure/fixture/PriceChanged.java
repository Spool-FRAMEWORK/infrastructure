package software.spool.infrastructure.fixture;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import software.spool.core.model.Event;
import software.spool.core.model.annotation.Delivery;
import software.spool.core.port.bus.DeliveryPolicy;

import java.time.Instant;

@Delivery(DeliveryPolicy.EVERY_SUBSCRIBER)
@JsonIgnoreProperties(ignoreUnknown = true)
public record PriceChanged(String payload) implements Event {
    @Override public String eventId() { return payload; }
    @Override public String causationId() { return ""; }
    @Override public String correlationId() { return ""; }
    @Override public Instant timestamp() { return Instant.EPOCH; }
}

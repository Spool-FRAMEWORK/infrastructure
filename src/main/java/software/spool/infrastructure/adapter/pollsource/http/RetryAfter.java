package software.spool.infrastructure.adapter.pollsource.http;

import java.net.http.HttpHeaders;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * How a source says how long to wait when it answers that it cannot take calls: where it says it, in what format,
 * and the longest wait to accept. A longer one is cut to that maximum.
 */
public record RetryAfter(RetryAfterLocation location, RetryAfterFormat format, Duration maximum) {
    public static final String DEFAULT_HEADER = "Retry-After";
    public static final Duration DEFAULT_MAXIMUM = Duration.ofHours(1);

    public static RetryAfter byDefault() {
        return new RetryAfter(new RetryAfterLocation.Header(DEFAULT_HEADER), RetryAfterFormat.SECONDS, DEFAULT_MAXIMUM);
    }

    /** The time to call the source again, when its answer says one that is still to come. */
    public Optional<Instant> waitUntil(HttpHeaders headers, byte[] body, Instant now) {
        Instant latest = now.plus(maximum);
        return location.find(headers, body)
                .flatMap(value -> format.read(value, now))
                .filter(now::isBefore)
                .map(until -> until.isAfter(latest) ? latest : until);
    }
}

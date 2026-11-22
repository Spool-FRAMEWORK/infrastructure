package software.spool.infrastructure.adapter.pollsource.http;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/** How a source writes the time to wait before it is called again. */
public enum RetryAfterFormat {
    /** Seconds to wait. A value that is not a number is read as an HTTP date, as Retry-After allows both. */
    SECONDS {
        @Override
        Optional<Instant> read(String value, Instant now) {
            return number(value).map(now::plusSeconds).or(() -> HTTP_DATE.read(value, now));
        }
    },
    /** An HTTP date, such as {@code Sun, 22 Nov 2026 10:00:03 GMT}. */
    HTTP_DATE {
        @Override
        Optional<Instant> read(String value, Instant now) {
            try {
                return Optional.of(ZonedDateTime.parse(value.strip(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
            } catch (DateTimeParseException notADate) {
                return Optional.empty();
            }
        }
    },
    /** The time to call again, in seconds since the epoch. */
    EPOCH_SECONDS {
        @Override
        Optional<Instant> read(String value, Instant now) {
            return number(value).map(Instant::ofEpochSecond);
        }
    },
    /** The time to call again, in milliseconds since the epoch. */
    EPOCH_MILLIS {
        @Override
        Optional<Instant> read(String value, Instant now) {
            return number(value).map(Instant::ofEpochMilli);
        }
    };

    abstract Optional<Instant> read(String value, Instant now);

    private static Optional<Long> number(String value) {
        try {
            return Optional.of(Long.parseLong(value.strip()));
        } catch (NumberFormatException notANumber) {
            return Optional.empty();
        }
    }
}

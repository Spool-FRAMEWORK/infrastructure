package software.spool.infrastructure.adapter.pollsource.http;

import org.junit.jupiter.api.Test;

import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RetryAfterTest {
    private static final Instant NOW = Instant.parse("2026-11-22T10:00:00Z");
    private static final byte[] NO_BODY = new byte[0];

    @Test
    void byDefault_readsTheRetryAfterHeaderInSeconds() {
        assertThat(RetryAfter.byDefault().waitUntil(headers("Retry-After", "3"), NO_BODY, NOW))
                .contains(NOW.plusSeconds(3));
    }

    @Test
    void byDefault_readsTheRetryAfterHeaderAsAnHttpDate() {
        String date = DateTimeFormatter.RFC_1123_DATE_TIME.format(NOW.plusSeconds(3).atOffset(ZoneOffset.UTC));

        assertThat(RetryAfter.byDefault().waitUntil(headers("Retry-After", date), NO_BODY, NOW))
                .contains(NOW.plusSeconds(3));
    }

    @Test
    void readsTheDeclaredHeaderInEpochSeconds() {
        RetryAfter retryAfter = new RetryAfter(new RetryAfterLocation.Header("X-RateLimit-Reset"),
                RetryAfterFormat.EPOCH_SECONDS, RetryAfter.DEFAULT_MAXIMUM);
        String reset = String.valueOf(NOW.plusSeconds(3).getEpochSecond());

        assertThat(retryAfter.waitUntil(headers("X-RateLimit-Reset", reset), NO_BODY, NOW)).contains(NOW.plusSeconds(3));
    }

    @Test
    void readsEpochMilliseconds() {
        RetryAfter retryAfter = new RetryAfter(new RetryAfterLocation.Header("X-RateLimit-Reset"),
                RetryAfterFormat.EPOCH_MILLIS, RetryAfter.DEFAULT_MAXIMUM);
        String reset = String.valueOf(NOW.plusSeconds(3).toEpochMilli());

        assertThat(retryAfter.waitUntil(headers("X-RateLimit-Reset", reset), NO_BODY, NOW)).contains(NOW.plusSeconds(3));
    }

    @Test
    void readsTheDeclaredFieldOfTheBody() {
        RetryAfter retryAfter = new RetryAfter(new RetryAfterLocation.BodyField("/error/retryInSeconds"),
                RetryAfterFormat.SECONDS, RetryAfter.DEFAULT_MAXIMUM);
        byte[] body = "{\"error\": {\"retryInSeconds\": 3}}".getBytes(StandardCharsets.UTF_8);

        assertThat(retryAfter.waitUntil(headers(), body, NOW)).contains(NOW.plusSeconds(3));
    }

    @Test
    void cutsAWaitLongerThanItsMaximum() {
        RetryAfter retryAfter = new RetryAfter(new RetryAfterLocation.Header("Retry-After"),
                RetryAfterFormat.SECONDS, Duration.ofSeconds(2));

        assertThat(retryAfter.waitUntil(headers("Retry-After", "3600"), NO_BODY, NOW)).contains(NOW.plusSeconds(2));
    }

    @Test
    void saysNothing_whenTheWaitIsMissingUnreadableOrPast() {
        RetryAfter retryAfter = RetryAfter.byDefault();
        String past = DateTimeFormatter.RFC_1123_DATE_TIME.format(NOW.minusSeconds(3).atOffset(ZoneOffset.UTC));

        assertThat(retryAfter.waitUntil(headers(), NO_BODY, NOW)).isEmpty();
        assertThat(retryAfter.waitUntil(headers("Retry-After", "soon"), NO_BODY, NOW)).isEmpty();
        assertThat(retryAfter.waitUntil(headers("Retry-After", past), NO_BODY, NOW)).isEmpty();
    }

    @Test
    void saysNothing_whenTheBodyIsNotJsonOrHasNoSuchField() {
        RetryAfter retryAfter = new RetryAfter(new RetryAfterLocation.BodyField("/error/retryInSeconds"),
                RetryAfterFormat.SECONDS, RetryAfter.DEFAULT_MAXIMUM);

        assertThat(retryAfter.waitUntil(headers(), "Too Many Requests".getBytes(StandardCharsets.UTF_8), NOW)).isEmpty();
        assertThat(retryAfter.waitUntil(headers(), "{}".getBytes(StandardCharsets.UTF_8), NOW)).isEmpty();
    }

    private static HttpHeaders headers(String... nameAndValue) {
        Map<String, List<String>> map = nameAndValue.length == 0
                ? Map.of()
                : Map.of(nameAndValue[0], List.of(nameAndValue[1]));
        return HttpHeaders.of(map, (name, value) -> true);
    }
}

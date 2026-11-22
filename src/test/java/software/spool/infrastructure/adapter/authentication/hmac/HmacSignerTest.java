package software.spool.infrastructure.adapter.authentication.hmac;

import org.junit.jupiter.api.Test;
import software.spool.core.port.authentication.Authentication;
import software.spool.core.port.http.HttpMethod;
import software.spool.core.port.http.HttpRequest;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class HmacSignerTest {
    private static final String HEADER = "X-Signature-256";
    private static final String TIMESTAMP_HEADER = "X-Timestamp";
    private static final Clock NOW = Clock.fixed(Instant.parse("2026-11-22T12:00:00Z"), ZoneOffset.UTC);
    private static final byte[] BODY = "{\"id\":\"node-a\"}".getBytes(StandardCharsets.UTF_8);

    @Test
    void headersFor_bodyWithATimestamp_areAcceptedByTheAuthenticatorWithTheSameSecret() {
        Map<String, String> headers = new HmacSigner(HEADER, "a-secret", Optional.of(TIMESTAMP_HEADER), NOW).headersFor(BODY);

        assertThat(authenticator("a-secret", Optional.of(TIMESTAMP_HEADER)).authenticate(request(headers)))
                .isInstanceOf(Authentication.Accepted.class);
    }

    @Test
    void headersFor_bodyWithoutATimestamp_areAcceptedByTheAuthenticatorWithTheSameSecret() {
        Map<String, String> headers = new HmacSigner(HEADER, "a-secret", Optional.empty(), NOW).headersFor(BODY);

        assertThat(headers).containsOnlyKeys(HEADER);
        assertThat(authenticator("a-secret", Optional.empty()).authenticate(request(headers)))
                .isInstanceOf(Authentication.Accepted.class);
    }

    @Test
    void headersFor_anotherSecret_areRejected() {
        Map<String, String> headers = new HmacSigner(HEADER, "a-secret", Optional.of(TIMESTAMP_HEADER), NOW).headersFor(BODY);

        assertThat(authenticator("another-secret", Optional.of(TIMESTAMP_HEADER)).authenticate(request(headers)))
                .isInstanceOf(Authentication.Rejected.class);
    }

    private static HmacSignatureRequestAuthenticator authenticator(String secret, Optional<String> timestampHeader) {
        return new HmacSignatureRequestAuthenticator("nodes", HEADER, secret, timestampHeader, Duration.ofMinutes(5),
                Set.of("node"), NOW);
    }

    private static HttpRequest request(Map<String, String> headers) {
        Map<String, List<String>> asLists = new HashMap<>();
        headers.forEach((name, value) -> asLists.put(name, List.of(value)));
        return new HttpRequest(HttpMethod.POST, "/spool/cluster/nodes", asLists, BODY);
    }
}

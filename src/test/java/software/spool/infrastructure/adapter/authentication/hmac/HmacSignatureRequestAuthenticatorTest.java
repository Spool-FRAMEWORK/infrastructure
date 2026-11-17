package software.spool.infrastructure.adapter.authentication.hmac;

import org.junit.jupiter.api.Test;
import software.spool.core.port.authentication.Authentication;
import software.spool.core.port.http.HttpMethod;
import software.spool.core.port.http.HttpRequest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class HmacSignatureRequestAuthenticatorTest {
    private static final String SECRET = "shared-webhook-secret";
    private static final String HEADER = "X-Signature-256";
    private static final String TIMESTAMP_HEADER = "X-Timestamp";
    private static final Instant NOW = Instant.parse("2026-11-17T12:00:00Z");
    private static final byte[] BODY = "{\"id\":\"boe-1\"}".getBytes(StandardCharsets.UTF_8);

    private final HmacSignatureRequestAuthenticator withoutTimestamp = new HmacSignatureRequestAuthenticator(
            "boe-webhook", HEADER, SECRET, Optional.empty(), Duration.ofMinutes(5), Set.of("ingest"), Clock.fixed(NOW, ZoneOffset.UTC));
    private final HmacSignatureRequestAuthenticator withTimestamp = new HmacSignatureRequestAuthenticator(
            "boe-webhook", HEADER, SECRET, Optional.of(TIMESTAMP_HEADER), Duration.ofMinutes(5), Set.of("ingest"), Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void correctSignature_isAcceptedWithTheConfiguredRoles() throws Exception {
        Authentication result = withoutTimestamp.authenticate(request(Map.of(HEADER, sign(SECRET, BODY)), BODY));

        assertThat(result).isInstanceOfSatisfying(Authentication.Accepted.class, accepted -> {
            assertThat(accepted.identity().subject()).isEqualTo("boe-webhook");
            assertThat(accepted.identity().roles()).containsExactly("ingest");
        });
    }

    @Test
    void signatureWithTheSha256Prefix_isAccepted() throws Exception {
        Authentication result = withoutTimestamp.authenticate(request(Map.of(HEADER, "sha256=" + sign(SECRET, BODY)), BODY));

        assertThat(result).isInstanceOf(Authentication.Accepted.class);
    }

    @Test
    void alteredBody_isRejected() throws Exception {
        String signature = sign(SECRET, BODY);

        Authentication result = withoutTimestamp.authenticate(request(Map.of(HEADER, signature), "{\"id\":\"boe-2\"}".getBytes()));

        assertThat(result).isInstanceOfSatisfying(Authentication.Rejected.class,
                rejected -> assertThat(rejected.reason()).doesNotContain(signature).doesNotContain(SECRET));
    }

    @Test
    void signatureMadeWithAnotherSecret_isRejected() throws Exception {
        assertThat(withoutTimestamp.authenticate(request(Map.of(HEADER, sign("another-secret", BODY)), BODY)))
                .isInstanceOf(Authentication.Rejected.class);
    }

    @Test
    void requestWithoutTheSignatureHeader_isRejected() {
        assertThat(withoutTimestamp.authenticate(request(Map.of(), BODY))).isInstanceOf(Authentication.Rejected.class);
    }

    @Test
    void timestampWithinTheTolerance_isAccepted() throws Exception {
        String timestamp = String.valueOf(NOW.minusSeconds(60).getEpochSecond());

        Authentication result = withTimestamp.authenticate(request(
                Map.of(HEADER, sign(SECRET, signedContent(timestamp)), TIMESTAMP_HEADER, timestamp), BODY));

        assertThat(result).isInstanceOf(Authentication.Accepted.class);
    }

    @Test
    void timestampOutsideTheTolerance_isRejectedEvenWithACorrectSignature() throws Exception {
        String timestamp = String.valueOf(NOW.minus(Duration.ofMinutes(10)).getEpochSecond());

        Authentication result = withTimestamp.authenticate(request(
                Map.of(HEADER, sign(SECRET, signedContent(timestamp)), TIMESTAMP_HEADER, timestamp), BODY));

        assertThat(result).isInstanceOf(Authentication.Rejected.class);
    }

    @Test
    void timestampThatWasNotPartOfTheSignature_isRejected() throws Exception {
        String timestamp = String.valueOf(NOW.getEpochSecond());

        Authentication result = withTimestamp.authenticate(request(Map.of(HEADER, sign(SECRET, BODY), TIMESTAMP_HEADER, timestamp), BODY));

        assertThat(result).isInstanceOf(Authentication.Rejected.class);
    }

    private static byte[] signedContent(String timestamp) {
        return (timestamp + "." + new String(BODY, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
    }

    private static String sign(String secret, byte[] content) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(content));
    }

    private static HttpRequest request(Map<String, String> headers, byte[] body) {
        Map<String, List<String>> multi = new java.util.HashMap<>();
        headers.forEach((name, value) -> multi.put(name, List.of(value)));
        return new HttpRequest(HttpMethod.POST, "/spool/webhooks/boe", multi, body);
    }
}

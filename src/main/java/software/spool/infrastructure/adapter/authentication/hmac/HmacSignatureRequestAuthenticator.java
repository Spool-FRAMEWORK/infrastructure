package software.spool.infrastructure.adapter.authentication.hmac;

import software.spool.core.port.authentication.Authentication;
import software.spool.core.port.authentication.Identity;
import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.core.port.http.HttpRequest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Set;

/**
 * Identifies a webhook sender by an HMAC-SHA256 signature of the body in a header, compared in constant time
 * (ADR-013). The signature is hexadecimal, with or without a {@code sha256=} prefix. With a timestamp header, the
 * signature covers {@code timestamp.body} and a request further from now than the tolerance is rejected, so a
 * captured request cannot be sent again later. A rejection never repeats the signature or the secret.
 */
public final class HmacSignatureRequestAuthenticator implements RequestAuthenticator {
    private static final String ALGORITHM = "HmacSHA256";
    private static final String PREFIX = "sha256=";

    private final String name;
    private final String header;
    private final SecretKeySpec key;
    private final Optional<String> timestampHeader;
    private final Duration tolerance;
    private final Set<String> roles;
    private final Clock clock;

    public HmacSignatureRequestAuthenticator(String name, String header, String secret, Optional<String> timestampHeader,
                                             Duration tolerance, Set<String> roles, Clock clock) {
        this.name = name;
        this.header = header;
        this.key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
        this.timestampHeader = timestampHeader;
        this.tolerance = tolerance;
        this.roles = Set.copyOf(roles);
        this.clock = clock;
    }

    @Override
    public Authentication authenticate(HttpRequest request) {
        Optional<String> signature = request.header(header);
        if (signature.isEmpty()) return Authentication.rejected("the request has no " + header + " header");
        byte[] provided;
        try {
            provided = HexFormat.of().parseHex(withoutPrefix(signature.get()));
        } catch (IllegalArgumentException e) {
            return Authentication.rejected("the " + header + " header is not a hexadecimal signature");
        }
        byte[] signedContent = request.body();
        if (timestampHeader.isPresent()) {
            Optional<String> timestamp = request.header(timestampHeader.get());
            if (timestamp.isEmpty()) return Authentication.rejected("the request has no " + timestampHeader.get() + " header");
            Optional<String> outOfTolerance = checkTimestamp(timestamp.get());
            if (outOfTolerance.isPresent()) return Authentication.rejected(outOfTolerance.get());
            signedContent = withTimestamp(timestamp.get(), signedContent);
        }
        if (!MessageDigest.isEqual(signatureOf(signedContent), provided)) {
            return Authentication.rejected("the " + header + " signature does not match the body");
        }
        return Authentication.accepted(new Identity(name, roles, name));
    }

    private Optional<String> checkTimestamp(String timestamp) {
        Instant sentAt;
        try {
            sentAt = Instant.ofEpochSecond(Long.parseLong(timestamp.trim()));
        } catch (NumberFormatException e) {
            return Optional.of("the " + timestampHeader.orElseThrow() + " header is not a number of seconds");
        }
        Duration distance = Duration.between(sentAt, clock.instant()).abs();
        if (distance.compareTo(tolerance) > 0) {
            return Optional.of("the request was signed " + distance.toSeconds() + " seconds away from now, beyond the tolerance of " + tolerance);
        }
        return Optional.empty();
    }

    private static byte[] withTimestamp(String timestamp, byte[] body) {
        byte[] prefix = (timestamp.trim() + ".").getBytes(StandardCharsets.UTF_8);
        byte[] content = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, content, 0, prefix.length);
        System.arraycopy(body, 0, content, prefix.length, body.length);
        return content;
    }

    private byte[] signatureOf(byte[] content) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(key);
            return mac.doFinal(content);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(ALGORITHM + " is not available in this JVM", e);
        }
    }

    private static String withoutPrefix(String signature) {
        String trimmed = signature.trim();
        return trimmed.startsWith(PREFIX) ? trimmed.substring(PREFIX.length()) : trimmed;
    }
}

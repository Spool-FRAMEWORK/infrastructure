package software.spool.infrastructure.adapter.authentication.hmac;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

/**
 * The HMAC-SHA256 of a body with a secret, alone or after a timestamp as {@code timestamp.body}: the one rule that
 * {@link HmacSigner} signs with and {@link HmacSignatureRequestAuthenticator} checks.
 */
final class HmacSha256 {
    static final String PREFIX = "sha256=";
    private static final String ALGORITHM = "HmacSHA256";

    private final SecretKeySpec key;

    HmacSha256(String secret) {
        this.key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
    }

    byte[] of(byte[] content) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(key);
            return mac.doFinal(content);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(ALGORITHM + " is not available in this JVM", e);
        }
    }

    static byte[] withTimestamp(String timestamp, byte[] body) {
        byte[] prefix = (timestamp.trim() + ".").getBytes(StandardCharsets.UTF_8);
        byte[] content = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, content, 0, prefix.length);
        System.arraycopy(body, 0, content, prefix.length, body.length);
        return content;
    }
}

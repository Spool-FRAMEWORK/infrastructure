package software.spool.infrastructure.adapter.authentication.hmac;

import java.time.Clock;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Signs the body of a request the way an {@code HMAC_SIGNATURE} provider with the same header, secret and timestamp
 * header checks it, for a Spool component that calls another one.
 */
public final class HmacSigner {
    private final String header;
    private final HmacSha256 hmac;
    private final Optional<String> timestampHeader;
    private final Clock clock;

    public HmacSigner(String header, String secret, Optional<String> timestampHeader, Clock clock) {
        this.header = header;
        this.hmac = new HmacSha256(secret);
        this.timestampHeader = timestampHeader;
        this.clock = clock;
    }

    /** The headers to send with this body: the signature and, if there is a timestamp header, the time it was signed. */
    public Map<String, String> headersFor(byte[] body) {
        Map<String, String> headers = new LinkedHashMap<>();
        byte[] signed = timestampHeader.map(name -> {
            String timestamp = String.valueOf(clock.instant().getEpochSecond());
            headers.put(name, timestamp);
            return HmacSha256.withTimestamp(timestamp, body);
        }).orElse(body);
        headers.put(header, HmacSha256.PREFIX + HexFormat.of().formatHex(hmac.of(signed)));
        return headers;
    }
}

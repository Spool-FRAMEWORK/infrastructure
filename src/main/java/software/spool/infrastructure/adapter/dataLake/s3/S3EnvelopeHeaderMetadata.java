package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.EnvelopeHeader;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * The header of an envelope kept as user metadata of its payload object. S3 only takes ASCII there, so the header is
 * written as JSON with its characters percent-encoded. S3 allows 2 KB of user metadata per object, counting keys and
 * values, and a header that does not fit is not written there.
 */
final class S3EnvelopeHeaderMetadata {
    private static final String HEADER_KEY = "spool-envelope-header";
    private static final int USER_METADATA_LIMIT_IN_BYTES = 2048;

    private S3EnvelopeHeaderMetadata() {
    }

    static Optional<Map<String, String>> metadataOf(EnvelopeHeader header) {
        String value = URLEncoder.encode(new String(RecordSerializerFactory.record().serialize(header), StandardCharsets.UTF_8),
                StandardCharsets.UTF_8);
        int size = HEADER_KEY.getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length;
        return size <= USER_METADATA_LIMIT_IN_BYTES ? Optional.of(Map.of(HEADER_KEY, value)) : Optional.empty();
    }

    static Optional<EnvelopeHeader> headerIn(Map<String, String> metadata) {
        return Optional.ofNullable(metadata.get(HEADER_KEY))
                .map(value -> URLDecoder.decode(value, StandardCharsets.UTF_8))
                .map(json -> PayloadDeserializerFactory.json().as(EnvelopeHeader.class)
                        .deserialize(json.getBytes(StandardCharsets.UTF_8)));
    }
}

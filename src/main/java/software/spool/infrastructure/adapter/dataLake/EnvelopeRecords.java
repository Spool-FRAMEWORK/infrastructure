package software.spool.infrastructure.adapter.dataLake;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.MediaType;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.mounter.api.model.GenericRecord;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The record a mounter reads for each envelope of the data lake: the fields of its header with its payload next to
 * them, a JSON payload as an object and any other as its bytes. A record whose payload is binary is kept with its
 * payload apart, as it came, and the rest of its fields next to it.
 */
public final class EnvelopeRecords {
    private static final String PAYLOAD = "payload";
    private static final String MEDIA_TYPE = "mediaType";
    private static final String MEDIA_TYPE_VALUE = "value";
    private static final PayloadDeserializer<Map<String, Object>> JSON = PayloadDeserializerFactory.json().asMap();

    private EnvelopeRecords() {
    }

    public static GenericRecord of(EnvelopeHeader header, byte[] payload) {
        Map<String, Object> fields = new LinkedHashMap<>(JSON.deserialize(RecordSerializerFactory.record().serialize(header)));
        fields.put(PAYLOAD, PayloadFormat.readable(header.mediaType(), payload));
        return GenericRecord.of(fields);
    }

    /**
     * Reads a record kept with its payload apart: the rest of its fields, and its payload as it came.
     */
    public static GenericRecord ofSeparated(byte[] otherFields, byte[] payload) {
        return headerOf(otherFields).withPayload(payload);
    }

    /**
     * Reads the fields of a record kept with its payload apart, to find its payload from its media type.
     */
    public static RecordHeader headerOf(byte[] otherFields) {
        return new RecordHeader(JSON.deserialize(otherFields));
    }

    /**
     * Reads a whole record in a single JSON: an envelope in the former format, or a record of silver. A payload kept
     * as text is its bytes in Base64, read as an object when its media type is JSON or missing, and as bytes
     * otherwise.
     */
    public static GenericRecord ofFormerFormat(byte[] envelope) {
        Map<String, Object> fields = JSON.deserialize(envelope);
        if (fields.get(PAYLOAD) instanceof String encoded) {
            fields.put(PAYLOAD, readable(fields, Base64.getDecoder().decode(encoded)));
        }
        return GenericRecord.of(fields);
    }

    /**
     * The binary payload of a record apart from the rest of its fields, when its payload is bytes and it has a media
     * type to name it by.
     */
    public static Optional<BinaryPayload> binaryPayloadOf(Map<String, Object> fields) {
        if (!(fields.get(PAYLOAD) instanceof byte[] payload)) return Optional.empty();
        return mediaTypeOf(fields).map(mediaType -> {
            Map<String, Object> otherFields = new LinkedHashMap<>(fields);
            otherFields.remove(PAYLOAD);
            return new BinaryPayload(otherFields, payload, mediaType);
        });
    }

    private static Object readable(Map<String, Object> fields, byte[] payload) {
        return mediaTypeOf(fields)
                .map(mediaType -> PayloadFormat.readable(mediaType, payload))
                .orElseGet(() -> JSON.deserialize(payload));
    }

    private static Optional<MediaType> mediaTypeOf(Map<String, Object> fields) {
        return fields.get(MEDIA_TYPE) instanceof Map<?, ?> mediaType
                && mediaType.get(MEDIA_TYPE_VALUE) instanceof String value && !value.isBlank()
                ? Optional.of(MediaType.of(value))
                : Optional.empty();
    }

    /**
     * A binary payload taken out of its record.
     *
     * @param otherFields the fields of the record without its payload
     * @param bytes       the payload as it came
     * @param mediaType   the media type of the payload
     */
    public record BinaryPayload(Map<String, Object> otherFields, byte[] bytes, MediaType mediaType) {
    }

    /**
     * The fields of a record whose payload is kept apart.
     */
    public static final class RecordHeader {
        private final Map<String, Object> fields;

        private RecordHeader(Map<String, Object> fields) {
            this.fields = fields;
        }

        public Optional<MediaType> mediaType() {
            return mediaTypeOf(fields);
        }

        public GenericRecord withPayload(byte[] payload) {
            Map<String, Object> joined = new LinkedHashMap<>(fields);
            joined.put(PAYLOAD, readable(fields, payload));
            return GenericRecord.of(joined);
        }
    }
}

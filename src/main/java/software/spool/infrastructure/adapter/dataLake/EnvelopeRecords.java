package software.spool.infrastructure.adapter.dataLake;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.mounter.api.model.GenericRecord;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The record a mounter reads for each envelope of the data lake: the fields of its header with its payload next to
 * them, a JSON payload as an object and any other as its bytes.
 */
public final class EnvelopeRecords {
    private static final String PAYLOAD = "payload";

    private EnvelopeRecords() {
    }

    public static GenericRecord of(EnvelopeHeader header, byte[] payload) {
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(header)));
        fields.put(PAYLOAD, PayloadFormat.readable(header.mediaType(), payload));
        return GenericRecord.of(fields);
    }

    /**
     * Reads a whole envelope in the former format, with its JSON payload in Base64, or a record that already has its
     * payload as an object.
     */
    public static GenericRecord ofFormerFormat(byte[] envelope) {
        Map<String, Object> fields = PayloadDeserializerFactory.json().asMap().deserialize(envelope);
        if (fields.get(PAYLOAD) instanceof String encoded) {
            fields.put(PAYLOAD, PayloadDeserializerFactory.json().asMap().deserialize(Base64.getDecoder().decode(encoded)));
        }
        return GenericRecord.of(fields);
    }
}

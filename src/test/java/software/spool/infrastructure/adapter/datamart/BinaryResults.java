package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.infrastructure.adapter.dataLake.EnvelopeRecords;
import software.spool.mounter.api.model.GenericRecord;

import java.time.Instant;

final class BinaryResults {
    static final byte[] PDF = {0x25, 0x50, 0x44, 0x46, (byte) 0xe2, (byte) 0xe3};
    private static final Instant CAPTURED_AT = Instant.parse("2026-11-22T10:00:00Z");

    private BinaryResults() {
    }

    static GenericRecord pdfRecord() {
        Envelope envelope = new Envelope(IdempotencyKey.of("report-1"), new EventMetadata().set(EventMetadataKey.SOURCE, "reports"),
                MediaType.of("application/pdf"), PDF, EnvelopeStatus.PERSISTED, 0, CAPTURED_AT, CAPTURED_AT);
        return EnvelopeRecords.of(envelope.header(), PDF);
    }
}

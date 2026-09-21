package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import software.spool.core.model.EnvelopeStatus;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
record S3EnvelopeDto(
        String idempotencyKey,
        byte[] metadata,
        byte[] payload,
        String status,
        int retries,
        Instant capturedAt,
        Instant updatedAt
) {
    S3EnvelopeDto movedTo(EnvelopeStatus newStatus, Instant movedAt) {
        return new S3EnvelopeDto(idempotencyKey, metadata, payload, newStatus.name(), retries, capturedAt, movedAt);
    }
}

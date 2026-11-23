package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.crawler.api.port.InboxWriter;

import java.util.Arrays;

public class S3InboxWriter implements InboxWriter {

    private static final String INBOX_PREFIX = "inbox/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    public S3InboxWriter(S3Client s3Client, String bucketName) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public IdempotencyKey receive(Envelope envelope) throws InboxWriteException, DuplicateEventException {
        String objectKey = INBOX_PREFIX + envelope.status().name() + "/" + envelope.idempotencyKey().value();

        if (existsInAnyStatus(envelope.idempotencyKey())) {
            throw new DuplicateEventException(envelope.idempotencyKey());
        }
        write(objectKey, envelope);
        return envelope.idempotencyKey();
    }

    /**
     * Writes the new envelope as captured before removing the one in quarantine, so a failure in between leaves both
     * rather than none.
     */
    @Override
    public Receipt receiveReplacingQuarantined(Envelope envelope) throws InboxWriteException, DuplicateEventException {
        String quarantined = INBOX_PREFIX + EnvelopeStatus.QUARANTINED.name() + "/" + envelope.idempotencyKey().value();
        if (!objectExists(quarantined)) return Receipt.stored(receive(envelope));
        try {
            byte[] stored = s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(quarantined).build())
                    .asByteArray();
            if (Arrays.equals(mapper.readValue(stored, S3EnvelopeDto.class).payload(), envelope.payload())) {
                throw new DuplicateEventException(envelope.idempotencyKey());
            }
        } catch (DuplicateEventException e) {
            throw e;
        } catch (S3Exception e) {
            throw new InboxWriteException("Failed to read the envelope in quarantine from S3: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new InboxWriteException("Failed to read the envelope in quarantine: " + e.getMessage(), e);
        }
        write(INBOX_PREFIX + EnvelopeStatus.CAPTURED.name() + "/" + envelope.idempotencyKey().value(), envelope);
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(quarantined).build());
        } catch (S3Exception e) {
            throw new InboxWriteException("Failed to remove the envelope in quarantine from S3: " + e.getMessage(), e);
        }
        return Receipt.replacedQuarantined(envelope.idempotencyKey());
    }

    private void write(String objectKey, Envelope envelope) {
        try {
            S3EnvelopeDto dto = new S3EnvelopeDto(
                    envelope.idempotencyKey().value(),
                    RecordSerializerFactory.record().serialize(envelope.metadata()),
                    envelope.payload(),
                    envelope.status().name(),
                    envelope.retries(),
                    envelope.capturedAt(),
                    envelope.updatedAt()
            );

            byte[] body = mapper.writeValueAsBytes(dto);

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromBytes(body)
            );
        } catch (S3Exception e) {
            throw new InboxWriteException("Failed to write envelope to S3: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new InboxWriteException("Failed to serialize envelope: " + e.getMessage(), e);
        }
    }

    private boolean existsInAnyStatus(IdempotencyKey idempotencyKey) {
        for (EnvelopeStatus status : EnvelopeStatus.values()) {
            if (objectExists(INBOX_PREFIX + status.name() + "/" + idempotencyKey.value())) return true;
        }
        return false;
    }

    private boolean objectExists(String key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()
            );
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }
}
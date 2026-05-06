package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.InboxUpdateException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxUpdater;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3InboxUpdater implements InboxUpdater {

    private static final String INBOX_PREFIX = "inbox/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    public S3InboxUpdater(S3Client s3Client, String bucketName) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Collection<Envelope> update(Collection<IdempotencyKey> idempotencyKeys,
                                       EnvelopeStatus newStatus) throws InboxUpdateException {
        List<Envelope> updated = new ArrayList<>();
        for (IdempotencyKey key : idempotencyKeys) {
            updated.add(moveToStatus(key, newStatus));
        }
        return updated;
    }

    private Envelope moveToStatus(IdempotencyKey idempotencyKey,
                                  EnvelopeStatus newStatus) throws InboxUpdateException {
        try {
            String sourceKey = findCurrentKey(idempotencyKey);
            if (sourceKey == null) {
                throw new InboxUpdateException(
                        idempotencyKey,
                        "Envelope not found in S3 inbox: " + idempotencyKey.value(),
                        null
                );
            }

            String destinationKey = INBOX_PREFIX + newStatus.name() + "/" + idempotencyKey.value();

            ResponseBytes<GetObjectResponse> raw = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(sourceKey)
                            .build()
            );
            EnvelopeDto dto = mapper.readValue(raw.asByteArray(), EnvelopeDto.class);

            s3Client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName).sourceKey(sourceKey)
                    .destinationBucket(bucketName).destinationKey(destinationKey)
                    .build()
            );
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName).key(sourceKey)
                    .build()
            );

            return toEnvelope(dto, idempotencyKey, newStatus);

        } catch (InboxUpdateException e) {
            throw e;
        } catch (Exception e) {
            throw new InboxUpdateException(idempotencyKey, e.getMessage(), e);
        }
    }

    private String findCurrentKey(IdempotencyKey idempotencyKey) {
        for (EnvelopeStatus status : EnvelopeStatus.values()) {
            String candidate = INBOX_PREFIX + status.name() + "/" + idempotencyKey.value();
            try {
                s3Client.headObject(HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(candidate)
                        .build()
                );
                return candidate;
            } catch (NoSuchKeyException ignored) {
            }
        }
        return null;
    }

    private Envelope toEnvelope(EnvelopeDto dto,
                                IdempotencyKey idempotencyKey,
                                EnvelopeStatus newStatus) throws Exception {
        EventMetadata metadata = PayloadDeserializerFactory.json()
                .as(EventMetadata.class)
                .deserialize(dto.metadata());

        return PayloadDeserializerFactory.json().as(Envelope.class)
                .deserialize(RecordSerializerFactory.record().serialize(dto)).withStatus(newStatus);
    }

    record EnvelopeDto(
            String idempotencyKey,
            String metadata,
            String payload,
            String status,
            int retries,
            Instant capturedAt
    ) {}
}
package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxEnvelopeRemover;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3InboxRemover implements InboxEnvelopeRemover {

    private static final String INBOX_PREFIX = "inbox/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    public S3InboxRemover(S3Client s3Client, String bucketName) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Collection<Envelope> remove(Collection<IdempotencyKey> idempotencyKeys) {
        List<Envelope> removed = new ArrayList<>();
        for (IdempotencyKey key : idempotencyKeys) {
            deleteAndCollect(key).ifPresent(removed::add);
        }
        return removed;
    }

    private java.util.Optional<Envelope> deleteAndCollect(IdempotencyKey idempotencyKey) {
        String s3Key = findCurrentKey(idempotencyKey);
        if (s3Key == null) return java.util.Optional.empty();

        try {
            ResponseBytes<GetObjectResponse> raw = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Key)
                            .build()
            );
            EnvelopeDto dto = mapper.readValue(raw.asByteArray(), EnvelopeDto.class);

            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build()
            );

            return java.util.Optional.of(toEnvelope(dto));

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to remove envelope [" + idempotencyKey.value() + "]: " + e.getMessage(), e);
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

    private Envelope toEnvelope(EnvelopeDto dto) throws Exception {
        EventMetadata metadata = PayloadDeserializerFactory.json()
                .as(EventMetadata.class)
                .deserialize(dto.metadata());

        return PayloadDeserializerFactory.json().as(Envelope.class)
                .deserialize(RecordSerializerFactory.record().serialize(dto));
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
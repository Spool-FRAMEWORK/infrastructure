package software.spool.infrastructure.adapter.inbox.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.InboxReadException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxReader;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class S3InboxReader implements InboxReader{
    private static final String INBOX_PREFIX = "inbox/";
    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    public S3InboxReader(S3Client s3Client, String bucketName) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Collection<Envelope> findByStatus(EnvelopeStatus status) throws InboxReadException {
        String prefix = INBOX_PREFIX + status.name() + "/";
        try {
            ListObjectsV2Response listing = s3Client.listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(prefix)
                            .build()
            );

            List<Envelope> envelopes = new ArrayList<>();
            for (S3Object s3Obj : listing.contents()) {
                EnvelopeDto dto = fetchDto(s3Obj.key());
                envelopes.add(toEnvelope(dto));
            }
            return envelopes;

        } catch (InboxReadException e) {
            throw e;
        } catch (Exception e) {
            throw new InboxReadException("Failed to query inbox by status [" + status + "]: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<Envelope> findById(IdempotencyKey idempotencyKey) throws InboxReadException {
        try {
            for (EnvelopeStatus status : EnvelopeStatus.values()) {
                String key = INBOX_PREFIX + status.name() + "/" + idempotencyKey.value();

                ListObjectsV2Response listing = s3Client.listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(bucketName)
                                .prefix(key)
                                .build()
                );

                Optional<S3Object> match = listing.contents().stream()
                        .filter(obj -> obj.key().equals(key))
                        .findFirst();

                if (match.isPresent()) {
                    EnvelopeDto dto = fetchDto(match.get().key());
                    return Optional.of(toEnvelope(dto));
                }
            }
            return Optional.empty();

        } catch (InboxReadException e) {
            throw e;
        } catch (Exception e) {
            throw new InboxReadException(
                    "Failed to resolve envelope by idempotency key [" + idempotencyKey.value() + "]: " + e.getMessage(), e);
        }
    }

    @Override
    public Collection<Envelope> findByIds(Collection<IdempotencyKey> idempotencyKeys) throws InboxReadException {
        List<Envelope> result = new ArrayList<>();
        for (IdempotencyKey key : idempotencyKeys) {
            findById(key).ifPresent(result::add);
        }
        return result;
    }

    private EnvelopeDto fetchDto(String s3Key) throws Exception {
        ResponseBytes<GetObjectResponse> raw = s3Client.getObjectAsBytes(
                GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .build()
        );
        return mapper.readValue(raw.asByteArray(), EnvelopeDto.class);
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
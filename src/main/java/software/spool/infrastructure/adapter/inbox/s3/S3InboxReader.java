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
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class S3InboxReader implements InboxReader{
    private static final String INBOX_PREFIX = "inbox/";
    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;
    private final BoundedConcurrency concurrency;

    public S3InboxReader(S3Client s3Client, String bucketName) {
        this(s3Client, bucketName, BoundedConcurrency.withThreads(Runtime.getRuntime().availableProcessors()));
    }

    public S3InboxReader(S3Client s3Client, String bucketName, BoundedConcurrency concurrency) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
        this.concurrency = concurrency;
    }

    @Override
    public Collection<Envelope> findByStatus(EnvelopeStatus status) throws InboxReadException {
        return read(status, object -> true);
    }

    @Override
    public Collection<Envelope> findByStatusModifiedBefore(EnvelopeStatus status, Instant limit) throws InboxReadException {
        return read(status, object -> object.lastModified().isBefore(limit));
    }

    private Collection<Envelope> read(EnvelopeStatus status, Predicate<S3Object> selection) throws InboxReadException {
        try {
            List<String> keys = listKeys(status, selection);
            return concurrency.map(keys, key -> envelopeAt(key, status)).toList();
        } catch (InboxReadException e) {
            throw e;
        } catch (Exception e) {
            throw new InboxReadException("Failed to query inbox by status [" + status + "]: " + e.getMessage(), e);
        }
    }

    private List<String> listKeys(EnvelopeStatus status, Predicate<S3Object> selection) {
        String prefix = INBOX_PREFIX + status.name() + "/";
        List<String> keys = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Response listing = s3Client.listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(prefix)
                            .continuationToken(continuationToken)
                            .build()
            );
            listing.contents().stream().filter(selection).map(S3Object::key).forEach(keys::add);
            continuationToken = Boolean.TRUE.equals(listing.isTruncated()) ? listing.nextContinuationToken() : null;
        } while (continuationToken != null);
        return keys;
    }

    private Envelope envelopeAt(String key, EnvelopeStatus status) {
        try {
            return toEnvelope(fetchDto(key), status);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read " + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<Envelope> findById(IdempotencyKey idempotencyKey) throws InboxReadException {
        try {
            for (EnvelopeStatus status : EnvelopeStatus.values()) {
                try {
                    S3EnvelopeDto dto = fetchDto(INBOX_PREFIX + status.name() + "/" + idempotencyKey.value());
                    return Optional.of(toEnvelope(dto, status));
                } catch (NoSuchKeyException ignored) {
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

    private S3EnvelopeDto fetchDto(String s3Key) throws Exception {
        ResponseBytes<GetObjectResponse> raw = s3Client.getObjectAsBytes(
                GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .build()
        );
        return mapper.readValue(raw.asByteArray(), S3EnvelopeDto.class);
    }

    private Envelope toEnvelope(S3EnvelopeDto dto, EnvelopeStatus status) throws Exception {
        EventMetadata eventMetadata = PayloadDeserializerFactory.json()
                .as(EventMetadata.class)
                .deserialize(dto.metadata());

        byte[] dtoBytes = RecordSerializerFactory.record().serialize(dto);
        com.fasterxml.jackson.databind.node.ObjectNode node =
                (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(dtoBytes);
        node.set("idempotencyKey", mapper.createObjectNode().put("value", dto.idempotencyKey()));
        node.set("metadata", mapper.valueToTree(eventMetadata));
        node.put("status", status.name());
        return PayloadDeserializerFactory.json().as(Envelope.class)
                .deserialize(mapper.writeValueAsBytes(node));
    }
}
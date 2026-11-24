package software.spool.infrastructure.adapter.inbox.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.ProcessedKey;
import software.spool.core.port.idempotency.ProcessedKeyRepository;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Keeps the processed keys next to the envelopes of an S3 inbox, one object per key holding the moment it was
 * processed, so they outlive a restart and every replica sees them.
 */
public final class S3ProcessedKeyRepository implements ProcessedKeyRepository {
    static final String PREFIX = "inbox/processed-keys/";

    private final S3Client s3Client;
    private final String bucketName;

    public S3ProcessedKeyRepository(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    @Override
    public boolean contains(IdempotencyKey key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(PREFIX + key.value()).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public void save(ProcessedKey processedKey) {
        try {
            s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(PREFIX + processedKey.key().value())
                            .contentType("text/plain").build(),
                    RequestBody.fromString(processedKey.processedAt().toString(), StandardCharsets.UTF_8));
        } catch (S3Exception e) {
            throw new InboxWriteException("The processed key " + processedKey.key().value() + " could not be kept: " + e.getMessage(), e);
        }
    }

    @Override
    public void forgetProcessedBefore(Instant moment) {
        String continuation = null;
        do {
            ListObjectsV2Response page = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucketName).prefix(PREFIX).continuationToken(continuation).build());
            page.contents().stream()
                    .map(S3Object::key)
                    .filter(objectKey -> processedAt(objectKey).isBefore(moment))
                    .forEach(objectKey -> s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build()));
            continuation = page.isTruncated() ? page.nextContinuationToken() : null;
        } while (continuation != null);
    }

    private Instant processedAt(String objectKey) {
        return Instant.parse(s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())
                .asString(StandardCharsets.UTF_8).strip());
    }
}

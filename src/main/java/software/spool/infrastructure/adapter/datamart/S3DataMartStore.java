package software.spool.infrastructure.adapter.datamart;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.spool.infrastructure.adapter.s3.S3ObjectKeys;

import java.util.List;
import java.util.Optional;

/** The objects of a data mart as objects of a bucket, keyed by their path. */
final class S3DataMartStore implements DataMartStore {
    private static final String SEPARATOR = "/";
    private static final String ANY_OBJECT = "*";
    private static final int PRECONDITION_FAILED = 412;
    private static final int CONFLICT = 409;

    private final S3Client s3Client;
    private final String bucket;

    S3DataMartStore(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public List<String> objectsIn(String folder) {
        String prefix = folder + SEPARATOR;
        return S3ObjectKeys.under(s3Client, bucket, prefix).stream()
                .filter(key -> !key.substring(prefix.length()).contains(SEPARATOR))
                .toList();
    }

    @Override
    public List<String> objectsUnder(String prefix) {
        return S3ObjectKeys.under(s3Client, bucket, prefix + SEPARATOR);
    }

    @Override
    public Optional<byte[]> read(String path) {
        try {
            return Optional.of(s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(path).build())
                    .asByteArray());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        }
    }

    @Override
    public void write(String path, byte[] content) {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .contentLength((long) content.length)
                        .build(),
                RequestBody.fromBytes(content));
    }

    @Override
    public boolean createIfAbsent(String path, byte[] content) {
        try {
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(path)
                            .ifNoneMatch(ANY_OBJECT)
                            .contentLength((long) content.length)
                            .build(),
                    RequestBody.fromBytes(content));
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == PRECONDITION_FAILED || e.statusCode() == CONFLICT) return false;
            throw e;
        }
    }

    @Override
    public void delete(String path) {
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(path).build());
    }
}

package software.spool.infrastructure.adapter.s3;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class InMemoryS3Client implements S3Client {
    public static final String HEAD = "head";
    public static final String GET = "get";
    public static final String PUT = "put";
    public static final String COPY = "copy";
    public static final String DELETE = "delete";
    public static final String LIST = "list";

    private static final int MAXIMUM_KEYS_PER_LISTING = 1000;

    private record StoredObject(byte[] content, Instant lastModified) {}

    private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> requests = new ConcurrentHashMap<>();

    public void put(String key) {
        objects.put(key, new StoredObject(new byte[0], Instant.now()));
    }

    public void setLastModified(String key, Instant lastModified) {
        objects.computeIfPresent(key, (existing, stored) -> new StoredObject(stored.content(), lastModified));
    }

    public Set<String> keys() {
        return Set.copyOf(objects.keySet());
    }

    public int requests(String operation) {
        AtomicInteger counter = requests.get(operation);
        return counter == null ? 0 : counter.get();
    }

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public void close() {
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest request) {
        count(HEAD);
        if (!objects.containsKey(request.key()))
            throw NoSuchKeyException.builder().message("Not found: " + request.key()).build();
        return HeadObjectResponse.builder().build();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
        count(PUT);
        try {
            objects.put(request.key(), new StoredObject(body.contentStreamProvider().newStream().readAllBytes(), Instant.now()));
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
        return PutObjectResponse.builder().build();
    }

    @Override
    public ResponseBytes<GetObjectResponse> getObjectAsBytes(GetObjectRequest request) {
        count(GET);
        StoredObject stored = objects.get(request.key());
        if (stored == null)
            throw NoSuchKeyException.builder().message("Not found: " + request.key()).build();
        return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), stored.content());
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest request) {
        count(COPY);
        objects.put(request.destinationKey(), new StoredObject(objects.get(request.sourceKey()).content(), Instant.now()));
        return CopyObjectResponse.builder().build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        count(LIST);
        String prefix = Objects.requireNonNullElse(request.prefix(), "");
        int maximum = request.maxKeys() == null ? MAXIMUM_KEYS_PER_LISTING : Math.min(request.maxKeys(), MAXIMUM_KEYS_PER_LISTING);
        List<String> matching = objects.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .filter(key -> request.continuationToken() == null || key.compareTo(request.continuationToken()) > 0)
                .sorted()
                .toList();
        List<String> page = matching.subList(0, Math.min(maximum, matching.size()));
        boolean truncated = matching.size() > page.size();
        List<S3Object> contents = page.stream()
                .map(key -> S3Object.builder().key(key).lastModified(objects.get(key).lastModified()).build())
                .toList();
        return ListObjectsV2Response.builder()
                .contents(contents)
                .keyCount(contents.size())
                .isTruncated(truncated)
                .nextContinuationToken(truncated ? page.get(page.size() - 1) : null)
                .build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
        count(DELETE);
        objects.remove(request.key());
        return DeleteObjectResponse.builder().build();
    }

    private void count(String operation) {
        requests.computeIfAbsent(operation, key -> new AtomicInteger()).incrementAndGet();
    }
}

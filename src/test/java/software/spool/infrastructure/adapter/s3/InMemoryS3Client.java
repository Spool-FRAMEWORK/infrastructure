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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryS3Client implements S3Client {
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

    public void put(String key) {
        objects.put(key, new byte[0]);
    }

    public Set<String> keys() {
        return Set.copyOf(objects.keySet());
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
        if (!objects.containsKey(request.key()))
            throw NoSuchKeyException.builder().message("Not found: " + request.key()).build();
        return HeadObjectResponse.builder().build();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
        try {
            objects.put(request.key(), body.contentStreamProvider().newStream().readAllBytes());
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
        return PutObjectResponse.builder().build();
    }

    @Override
    public ResponseBytes<GetObjectResponse> getObjectAsBytes(GetObjectRequest request) {
        byte[] content = objects.get(request.key());
        if (content == null)
            throw NoSuchKeyException.builder().message("Not found: " + request.key()).build();
        return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), content);
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest request) {
        objects.put(request.destinationKey(), objects.get(request.sourceKey()));
        return CopyObjectResponse.builder().build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        List<S3Object> contents = objects.keySet().stream()
                .filter(key -> key.startsWith(request.prefix()))
                .sorted()
                .map(key -> S3Object.builder().key(key).build())
                .toList();
        return ListObjectsV2Response.builder().contents(contents).build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
        objects.remove(request.key());
        return DeleteObjectResponse.builder().build();
    }
}

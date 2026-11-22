package software.spool.infrastructure.adapter.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.List;

/** Lists the keys of the objects of a bucket under a prefix, following every page of the listing. */
public final class S3ObjectKeys {
    private S3ObjectKeys() {
    }

    public static List<String> under(S3Client s3Client, String bucket, String prefix) {
        List<String> keys = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Response listing = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .continuationToken(continuationToken)
                    .build());
            listing.contents().stream().map(S3Object::key).forEach(keys::add);
            continuationToken = Boolean.TRUE.equals(listing.isTruncated()) ? listing.nextContinuationToken() : null;
        } while (continuationToken != null);
        return keys;
    }

    public static boolean anyUnder(S3Client s3Client, String bucket, String prefix) {
        return !s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1).build())
                .contents().isEmpty();
    }
}

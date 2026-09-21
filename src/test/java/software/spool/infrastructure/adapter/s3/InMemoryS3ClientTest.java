package software.spool.infrastructure.adapter.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryS3ClientTest {

    private static final String BUCKET = "bucket";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Test
    void listObjectsV2_moreThanAThousandKeys_returnsAPageAndAContinuationToken() {
        putKeys(2500);

        ListObjectsV2Response first = list("inbox/", null);

        assertThat(first.contents()).hasSize(1000);
        assertThat(first.isTruncated()).isTrue();
        assertThat(first.nextContinuationToken()).isEqualTo(first.contents().get(999).key());
    }

    @Test
    void listObjectsV2_followingTheToken_returnsEveryKeyOnce() {
        putKeys(2500);

        List<String> collected = new ArrayList<>();
        String token = null;
        int listings = 0;
        do {
            ListObjectsV2Response page = list("inbox/", token);
            page.contents().stream().map(S3Object::key).forEach(collected::add);
            token = page.isTruncated() ? page.nextContinuationToken() : null;
            listings++;
        } while (token != null);

        assertThat(collected).hasSize(2500).doesNotHaveDuplicates().isSorted();
        assertThat(listings).isEqualTo(3);
    }

    @Test
    void listObjectsV2_aFolderThatFitsInOnePage_isNotTruncated() {
        putKeys(10);

        ListObjectsV2Response page = list("inbox/", null);

        assertThat(page.contents()).hasSize(10);
        assertThat(page.isTruncated()).isFalse();
        assertThat(page.nextContinuationToken()).isNull();
    }

    @Test
    void listObjectsV2_onlyReturnsTheKeysWithThePrefix() {
        s3Client.put("inbox/CAPTURED/a");
        s3Client.put("inbox/PERSISTED/b");

        assertThat(list("inbox/CAPTURED/", null).contents()).extracting(S3Object::key).containsExactly("inbox/CAPTURED/a");
    }

    @Test
    void listObjectsV2_respectsAMaximumSmallerThanThePageSize() {
        putKeys(50);

        ListObjectsV2Response page = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET).prefix("inbox/").maxKeys(20).build());

        assertThat(page.contents()).hasSize(20);
        assertThat(page.isTruncated()).isTrue();
    }

    @Test
    void listObjectsV2_returnsWhenEachObjectWasLastWritten() {
        s3Client.put("inbox/CAPTURED/a");
        Instant longAgo = Instant.parse("2026-01-01T00:00:00Z");
        s3Client.setLastModified("inbox/CAPTURED/a", longAgo);

        assertThat(list("inbox/", null).contents().get(0).lastModified()).isEqualTo(longAgo);
    }

    @Test
    void putObject_setsTheLastModifiedToNow() {
        Instant before = Instant.now();

        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("inbox/CAPTURED/a").build(), RequestBody.fromString("{}"));

        assertThat(list("inbox/", null).contents().get(0).lastModified()).isBetween(before, Instant.now());
    }

    @Test
    void requests_countsEachOperationSeparately() {
        s3Client.put("inbox/CAPTURED/a");
        list("inbox/", null);
        list("inbox/", null);
        s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key("inbox/CAPTURED/a").build());

        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isEqualTo(2);
        assertThat(s3Client.requests(InMemoryS3Client.GET)).isEqualTo(1);
        assertThat(s3Client.requests(InMemoryS3Client.HEAD)).isZero();
    }

    private void putKeys(int count) {
        for (int index = 0; index < count; index++) {
            s3Client.put(String.format("inbox/CAPTURED/key-%05d", index));
        }
    }

    private ListObjectsV2Response list(String prefix, String continuationToken) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET).prefix(prefix).continuationToken(continuationToken).build());
    }
}

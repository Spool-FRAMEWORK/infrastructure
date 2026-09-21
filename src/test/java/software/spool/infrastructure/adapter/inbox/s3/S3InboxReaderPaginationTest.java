package software.spool.infrastructure.adapter.inbox.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.exception.InboxReadException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3InboxReaderPaginationTest {

    private static final String BUCKET = "spool-inbox-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);
    private final S3InboxReader reader = new S3InboxReader(s3Client, BUCKET);

    @Test
    void findByStatus_moreThanAThousandEnvelopes_returnsAllOfThem() throws Exception {
        receive(1500);

        Collection<Envelope> found = reader.findByStatus(EnvelopeStatus.CAPTURED);

        assertThat(found).hasSize(1500);
        assertThat(found).extracting(envelope -> envelope.idempotencyKey().value()).containsExactlyElementsOf(keys(1500));
    }

    @Test
    void findByStatus_aFolderThatFitsInOnePage_listsOnce() throws Exception {
        receive(10);

        reader.findByStatus(EnvelopeStatus.CAPTURED);

        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isEqualTo(1);
    }

    @Test
    void findByStatus_moreThanAThousandEnvelopes_listsOncePerPage() throws Exception {
        receive(2500);

        reader.findByStatus(EnvelopeStatus.CAPTURED);

        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isEqualTo(3);
    }

    @Test
    void findByStatusModifiedBefore_onlyDownloadsWhatIsOldEnough() throws Exception {
        receive(1500);
        Instant longAgo = Instant.now().minus(Duration.ofDays(2));
        keys(1500).subList(0, 10).forEach(key -> s3Client.setLastModified("inbox/CAPTURED/" + key, longAgo));
        int downloadsBefore = s3Client.requests(InMemoryS3Client.GET);

        Collection<Envelope> found = reader.findByStatusModifiedBefore(EnvelopeStatus.CAPTURED, Instant.now().minus(Duration.ofDays(1)));

        assertThat(found).hasSize(10);
        assertThat(s3Client.requests(InMemoryS3Client.GET) - downloadsBefore).isEqualTo(10);
        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isEqualTo(2);
    }

    @Test
    void findByStatusModifiedBefore_nothingOldEnough_downloadsNothing() throws Exception {
        receive(50);
        int downloadsBefore = s3Client.requests(InMemoryS3Client.GET);

        Collection<Envelope> found = reader.findByStatusModifiedBefore(EnvelopeStatus.CAPTURED, Instant.now().minus(Duration.ofDays(1)));

        assertThat(found).isEmpty();
        assertThat(s3Client.requests(InMemoryS3Client.GET) - downloadsBefore).isZero();
    }

    @Test
    void findByStatusModifiedBefore_everythingOld_returnsEverythingLikeFindByStatus() throws Exception {
        receive(1200);

        Collection<Envelope> found = reader.findByStatusModifiedBefore(EnvelopeStatus.CAPTURED, Instant.now().plus(Duration.ofDays(1)));

        assertThat(found).hasSize(1200);
    }

    @Test
    void findByStatusModifiedBefore_onlyLooksInTheAskedStatus() throws Exception {
        receive(5);
        new S3InboxUpdater(s3Client, BUCKET).update(List.of(IdempotencyKey.of("key-00000")), EnvelopeStatus.PERSISTED);
        s3Client.setLastModified("inbox/PERSISTED/key-00000", Instant.now().minus(Duration.ofDays(5)));

        Collection<Envelope> found = reader.findByStatusModifiedBefore(EnvelopeStatus.CAPTURED, Instant.now().plus(Duration.ofDays(1)));

        assertThat(found).extracting(envelope -> envelope.idempotencyKey().value())
                .containsExactly("key-00001", "key-00002", "key-00003", "key-00004");
    }

    @Test
    void findByStatus_anObjectThatCannotBeRead_failsNamingTheKey() {
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("inbox/CAPTURED/broken").build(),
                RequestBody.fromString("this is not an envelope"));

        assertThatThrownBy(() -> reader.findByStatus(EnvelopeStatus.CAPTURED))
                .isInstanceOf(InboxReadException.class)
                .hasMessageContaining("inbox/CAPTURED/broken");
    }

    private void receive(int count) throws Exception {
        for (String key : keys(count)) {
            writer.receive(capturedEnvelope(key));
        }
    }

    private static List<String> keys(int count) {
        return IntStream.range(0, count).mapToObj(index -> String.format("key-%05d", index)).toList();
    }

    private static Envelope capturedEnvelope(String key) {
        return new Envelope(
                IdempotencyKey.of(key),
                new EventMetadata(),
                MediaTypes.JSON,
                "{}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                0,
                Instant.now(),
                null);
    }
}

package software.spool.infrastructure.adapter.inbox.s3;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class S3InboxReaderFindByIdTest {

    private static final String BUCKET = "spool-inbox-test";
    private static final String KEY = "looked-up-key";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);
    private final S3InboxUpdater updater = new S3InboxUpdater(s3Client, BUCKET);
    private final S3InboxReader reader = new S3InboxReader(s3Client, BUCKET);

    @Test
    void findById_anEnvelopeInTheFirstStatus_takesOneDownloadAndNoListing() throws Exception {
        writer.receive(capturedEnvelope());
        int downloadsBefore = s3Client.requests(InMemoryS3Client.GET);

        Optional<Envelope> found = reader.findById(IdempotencyKey.of(KEY));

        assertThat(found).isPresent();
        assertThat(s3Client.requests(InMemoryS3Client.GET) - downloadsBefore).isEqualTo(1);
        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isZero();
    }

    @Test
    void findById_anEnvelopeInALaterStatus_asksOncePerStatusUntilItIsFoundAndNeverListsTheFolder() throws Exception {
        writer.receive(capturedEnvelope());
        updater.update(List.of(IdempotencyKey.of(KEY)), EnvelopeStatus.PERSISTED);
        int downloadsBefore = s3Client.requests(InMemoryS3Client.GET);

        Optional<Envelope> found = reader.findById(IdempotencyKey.of(KEY));

        assertThat(found).isPresent();
        assertThat(found.get().status()).isEqualTo(EnvelopeStatus.PERSISTED);
        assertThat(s3Client.requests(InMemoryS3Client.GET) - downloadsBefore).isEqualTo(3);
        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isZero();
    }

    @Test
    void findById_aKeyThatIsNotThere_isEmptyAfterOneRequestPerStatusAndNoListing() throws Exception {
        Optional<Envelope> found = reader.findById(IdempotencyKey.of("missing"));

        assertThat(found).isEmpty();
        assertThat(s3Client.requests(InMemoryS3Client.GET)).isEqualTo(EnvelopeStatus.values().length);
        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isZero();
    }

    @Test
    void findByIds_severalKeys_neverListsTheFolder() throws Exception {
        writer.receive(capturedEnvelope());

        assertThat(reader.findByIds(List.of(IdempotencyKey.of(KEY), IdempotencyKey.of("missing")))).hasSize(1);
        assertThat(s3Client.requests(InMemoryS3Client.LIST)).isZero();
    }

    private static Envelope capturedEnvelope() {
        return new Envelope(
                IdempotencyKey.of(KEY),
                new EventMetadata(),
                MediaTypes.JSON,
                "{}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                0,
                Instant.now(),
                null);
    }
}

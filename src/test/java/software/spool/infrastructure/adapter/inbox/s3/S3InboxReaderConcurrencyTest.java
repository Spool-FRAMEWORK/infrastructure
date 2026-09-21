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
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3InboxReaderConcurrencyTest {

    private static final String BUCKET = "spool-inbox-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final S3InboxWriter writer = new S3InboxWriter(s3Client, BUCKET);

    @Test
    void findByStatus_severalThreads_returnsTheSameEnvelopesInTheSameOrderAsOneThread() throws Exception {
        receive(300);

        try (BoundedConcurrency oneThread = BoundedConcurrency.withThreads(1);
             BoundedConcurrency severalThreads = BoundedConcurrency.withThreads(8, 4)) {
            List<String> sequential = describe(new S3InboxReader(s3Client, BUCKET, oneThread).findByStatus(EnvelopeStatus.CAPTURED));
            List<String> concurrent = describe(new S3InboxReader(s3Client, BUCKET, severalThreads).findByStatus(EnvelopeStatus.CAPTURED));

            assertThat(concurrent).hasSize(300).isEqualTo(sequential);
        }
    }

    @Test
    void findByStatusModifiedBefore_severalThreads_returnsTheSameAsOneThread() throws Exception {
        receive(300);
        Instant longAgo = Instant.now().minus(Duration.ofDays(2));
        IntStream.range(0, 300).filter(index -> index % 3 == 0)
                .forEach(index -> s3Client.setLastModified(String.format("inbox/CAPTURED/key-%05d", index), longAgo));
        Instant limit = Instant.now().minus(Duration.ofDays(1));

        try (BoundedConcurrency oneThread = BoundedConcurrency.withThreads(1);
             BoundedConcurrency severalThreads = BoundedConcurrency.withThreads(8)) {
            List<String> sequential = describe(new S3InboxReader(s3Client, BUCKET, oneThread).findByStatusModifiedBefore(EnvelopeStatus.CAPTURED, limit));
            List<String> concurrent = describe(new S3InboxReader(s3Client, BUCKET, severalThreads).findByStatusModifiedBefore(EnvelopeStatus.CAPTURED, limit));

            assertThat(concurrent).hasSize(100).isEqualTo(sequential);
        }
    }

    @Test
    void findByStatus_severalThreadsAndABrokenObject_stillNamesTheKey() throws Exception {
        receive(100);
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("inbox/CAPTURED/key-00050-broken").build(),
                RequestBody.fromString("this is not an envelope"));

        try (BoundedConcurrency severalThreads = BoundedConcurrency.withThreads(8)) {
            S3InboxReader reader = new S3InboxReader(s3Client, BUCKET, severalThreads);

            assertThatThrownBy(() -> reader.findByStatus(EnvelopeStatus.CAPTURED))
                    .isInstanceOf(InboxReadException.class)
                    .hasMessageContaining("inbox/CAPTURED/key-00050-broken");
        }
    }

    private static List<String> describe(Collection<Envelope> envelopes) {
        return envelopes.stream()
                .map(envelope -> envelope.idempotencyKey().value() + ":" + new String(envelope.payload(), StandardCharsets.UTF_8))
                .toList();
    }

    private void receive(int count) throws Exception {
        for (int index = 0; index < count; index++) {
            writer.receive(new Envelope(
                    IdempotencyKey.of(String.format("key-%05d", index)),
                    new EventMetadata(),
                    MediaTypes.JSON,
                    "{}".getBytes(StandardCharsets.UTF_8),
                    EnvelopeStatus.CAPTURED,
                    0,
                    Instant.now(),
                    null));
        }
    }
}

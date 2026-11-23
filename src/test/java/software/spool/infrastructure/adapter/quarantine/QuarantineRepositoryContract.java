package software.spool.infrastructure.adapter.quarantine;

import org.junit.jupiter.api.Test;
import software.spool.core.exception.QuarantineConflictException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantinePage;
import software.spool.core.model.quarantine.QuarantinePageRequest;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantineResolution;
import software.spool.core.model.quarantine.Resolver;
import software.spool.core.model.quarantine.QuarantineStatus;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Rules every {@link QuarantineRepository} adapter must follow.
 */
public abstract class QuarantineRepositoryContract {

    private static final IdempotencyKey FIRST_KEY = IdempotencyKey.of("first-key");
    private static final IdempotencyKey SECOND_KEY = IdempotencyKey.of("second-key");
    private static final Instant QUARANTINED_AT = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final Instant RESOLVED_AT = QUARANTINED_AT.plusSeconds(60);
    private static final Resolver ANA = Resolver.identified("ana", "people");
    private static final Resolver LUIS = Resolver.identified("luis", "people");

    protected abstract QuarantineRepository repository();

    @Test
    void find_savedRecord_returnsTheEnvelopeLocationReasonViolationsAndDate() {
        repository().save(validationRecord(FIRST_KEY));

        Optional<QuarantinedRecord> found = repository().find(FIRST_KEY);

        assertThat(found).isPresent();
        QuarantinedRecord quarantinedRecord = found.get();
        assertThat(quarantinedRecord.idempotencyKey()).isEqualTo(FIRST_KEY);
        assertThat(quarantinedRecord.envelope().payload()).isEqualTo("{\"price\":-1}".getBytes(StandardCharsets.UTF_8));
        assertThat(quarantinedRecord.envelope().status()).isEqualTo(EnvelopeStatus.CAPTURED);
        assertThat(quarantinedRecord.envelope().metadata().get(EventMetadataKey.SOURCE)).isEqualTo("boe");
        assertThat(quarantinedRecord.location()).isEqualTo(QuarantineLocation.inbox());
        assertThat(quarantinedRecord.status()).isEqualTo(QuarantineStatus.PENDING);
        assertThat(quarantinedRecord.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
        assertThat(quarantinedRecord.violations()).containsExactly("price must be positive");
        assertThat(quarantinedRecord.quarantinedAt()).isEqualTo(QUARANTINED_AT);
    }

    @Test
    void find_recordThatStayedInBronze_keepsItsPartition() {
        PartitionKey partition = PartitionKey.ofEntriesWithoutDate().with("source", "boe").build();
        repository().save(QuarantinedRecord.pending(envelope(FIRST_KEY), QuarantineLocation.bronze(partition),
                QuarantineReason.VALIDATION_FAILED, List.of(), QUARANTINED_AT));

        assertThat(repository().find(FIRST_KEY)).map(QuarantinedRecord::location).contains(QuarantineLocation.bronze(partition));
    }

    @Test
    void find_keyWithoutRecord_returnsEmpty() {
        assertThat(repository().find(FIRST_KEY)).isEmpty();
    }

    @Test
    void save_sameKeyTwice_keepsOnlyTheLastRecord() {
        repository().save(retriesRecord(FIRST_KEY));
        repository().save(validationRecord(FIRST_KEY));

        assertThat(repository().findAll()).hasSize(1);
        assertThat(repository().find(FIRST_KEY)).map(QuarantinedRecord::reason).contains(QuarantineReason.VALIDATION_FAILED);
    }

    @Test
    void save_resolvedRecord_isFoundByItsNewStatusAndNoLongerAsPending() {
        repository().save(validationRecord(FIRST_KEY));

        repository().save(validationRecord(FIRST_KEY).resolve(QuarantineResolution.purged(RESOLVED_AT, Resolver.unknown())));

        assertThat(repository().findByStatus(QuarantineStatus.PENDING)).isEmpty();
        assertThat(repository().findByStatus(QuarantineStatus.PURGED)).extracting(QuarantinedRecord::idempotencyKey)
                .containsExactly(FIRST_KEY);
        assertThat(repository().findAll()).hasSize(1);
    }

    @Test
    void save_reopenedRecord_keepsItsWholeHistory() {
        QuarantinedRecord reopened = validationRecord(FIRST_KEY)
                .resolve(QuarantineResolution.corrected(RESOLVED_AT, SECOND_KEY, Resolver.unknown()))
                .reenter(QuarantineReason.RETRIES_EXHAUSTED, List.of(), RESOLVED_AT);
        repository().save(validationRecord(FIRST_KEY).resolve(QuarantineResolution.corrected(RESOLVED_AT, SECOND_KEY, Resolver.unknown())));

        repository().save(reopened);

        assertThat(repository().find(FIRST_KEY)).map(QuarantinedRecord::history).contains(reopened.history());
        assertThat(repository().findByStatus(QuarantineStatus.PENDING)).hasSize(1);
        assertThat(repository().findByStatus(QuarantineStatus.CORRECTED)).isEmpty();
    }

    @Test
    void findByStatus_recordsInSeveralStatuses_returnsOnlyThoseInTheGivenOne() {
        repository().save(validationRecord(FIRST_KEY));
        repository().save(retriesRecord(SECOND_KEY).resolve(QuarantineResolution.republished(RESOLVED_AT, Resolver.unknown())));

        assertThat(repository().findByStatus(QuarantineStatus.PENDING)).extracting(QuarantinedRecord::idempotencyKey)
                .containsExactly(FIRST_KEY);
        assertThat(repository().findByStatus(QuarantineStatus.REPUBLISHED)).extracting(QuarantinedRecord::idempotencyKey)
                .containsExactly(SECOND_KEY);
    }

    @Test
    void findAll_twoSavedRecords_returnsBoth() {
        repository().save(validationRecord(FIRST_KEY));
        repository().save(retriesRecord(SECOND_KEY).resolve(QuarantineResolution.republished(RESOLVED_AT, Resolver.unknown())));

        assertThat(repository().findAll()).extracting(QuarantinedRecord::idempotencyKey)
                .containsExactlyInAnyOrder(FIRST_KEY, SECOND_KEY);
    }

    @Test
    void delete_savedRecord_removesOnlyThatOne() {
        repository().save(validationRecord(FIRST_KEY));
        repository().save(retriesRecord(SECOND_KEY));

        repository().delete(FIRST_KEY);

        assertThat(repository().find(FIRST_KEY)).isEmpty();
        assertThat(repository().find(SECOND_KEY)).isPresent();
    }

    @Test
    void delete_resolvedRecord_removesIt() {
        repository().save(validationRecord(FIRST_KEY).resolve(QuarantineResolution.purged(RESOLVED_AT, Resolver.unknown())));

        repository().delete(FIRST_KEY);

        assertThat(repository().findAll()).isEmpty();
    }

    @Test
    void delete_keyWithoutRecord_doesNothing() {
        repository().delete(FIRST_KEY);

        assertThat(repository().findAll()).isEmpty();
    }

    @Test
    void resolve_pendingRecord_keepsItInItsNewStatusWithWhoResolvedIt() {
        repository().save(validationRecord(FIRST_KEY));

        repository().resolve(validationRecord(FIRST_KEY).resolve(QuarantineResolution.purged(RESOLVED_AT, ANA)));

        assertThat(repository().find(FIRST_KEY)).hasValueSatisfying(found ->
                assertThat(found.resolution()).isEqualTo(QuarantineResolution.purged(RESOLVED_AT, ANA)));
        assertThat(repository().findByStatus(QuarantineStatus.PENDING)).isEmpty();
    }

    @Test
    void resolve_recordAlreadyResolved_failsWithAConflictAndKeepsTheFirstResolution() {
        repository().save(validationRecord(FIRST_KEY));
        repository().resolve(validationRecord(FIRST_KEY).resolve(QuarantineResolution.purged(RESOLVED_AT, ANA)));

        assertThatThrownBy(() -> repository().resolve(validationRecord(FIRST_KEY)
                .resolve(QuarantineResolution.republished(RESOLVED_AT, LUIS))))
                .isInstanceOf(QuarantineConflictException.class)
                .hasMessageContaining(FIRST_KEY.value());
        assertThat(repository().find(FIRST_KEY).orElseThrow().status()).isEqualTo(QuarantineStatus.PURGED);
    }

    @Test
    void resolve_keyNotInQuarantine_failsWithAConflict() {
        assertThatThrownBy(() -> repository().resolve(validationRecord(FIRST_KEY)
                .resolve(QuarantineResolution.purged(RESOLVED_AT, ANA))))
                .isInstanceOf(QuarantineConflictException.class);
        assertThat(repository().find(FIRST_KEY)).isEmpty();
    }

    @Test
    void resolve_sameRecordFromManyThreadsAtOnce_succeedsOnlyOnce() throws Exception {
        repository().save(validationRecord(FIRST_KEY));
        int attempts = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService threads = Executors.newFixedThreadPool(attempts);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int attempt = 0; attempt < attempts; attempt++) {
            results.add(threads.submit(() -> {
                start.await();
                try {
                    repository().resolve(validationRecord(FIRST_KEY).resolve(QuarantineResolution.purged(RESOLVED_AT, ANA)));
                    return true;
                } catch (QuarantineConflictException e) {
                    return false;
                }
            }));
        }
        start.countDown();
        int succeeded = 0;
        for (Future<Boolean> result : results) {
            if (result.get(10, TimeUnit.SECONDS)) succeeded++;
        }
        threads.shutdownNow();

        assertThat(succeeded).isEqualTo(1);
        assertThat(repository().find(FIRST_KEY).orElseThrow().history()).hasSize(1);
    }

    @Test
    void resolve_sameRecordFromTwoThreadsAtOnceManyTimes_theLoserAlwaysGetsAConflict() throws Exception {
        int rounds = 1000;
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < rounds; round++) {
                IdempotencyKey key = IdempotencyKey.of("race-" + round);
                repository().save(validationRecord(key));
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Boolean>> results = new ArrayList<>();
                for (int attempt = 0; attempt < 2; attempt++) {
                    results.add(threads.submit(() -> {
                        start.await();
                        try {
                            repository().resolve(validationRecord(key).resolve(QuarantineResolution.purged(RESOLVED_AT, ANA)));
                            return true;
                        } catch (QuarantineConflictException e) {
                            return false;
                        }
                    }));
                }
                start.countDown();
                int succeeded = 0;
                for (Future<Boolean> result : results) {
                    if (result.get(10, TimeUnit.SECONDS)) succeeded++;
                }
                assertThat(succeeded).as("round %d", round).isEqualTo(1);
                assertThat(repository().find(key).orElseThrow().history()).hasSize(1);
            }
        } finally {
            threads.shutdownNow();
        }
    }

    @Test
    void findByStatus_whileARecordOfTheStatusIsResolvedManyTimes_listsTheOthersWithoutFailing() throws Exception {
        int rounds = 1000;
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < rounds; round++) {
                IdempotencyKey resolved = IdempotencyKey.of("a-" + round);
                IdempotencyKey kept = IdempotencyKey.of("b-" + round);
                repository().save(validationRecord(resolved));
                repository().save(validationRecord(kept));
                CountDownLatch start = new CountDownLatch(1);
                Future<List<QuarantinedRecord>> listing = threads.submit(() -> {
                    start.await();
                    List<QuarantinedRecord> listed = new ArrayList<>(repository().findByStatus(QuarantineStatus.PENDING));
                    listed.addAll(everyPageOf(QuarantineStatus.PENDING));
                    return listed;
                });
                Future<?> resolution = threads.submit(() -> {
                    start.await();
                    repository().resolve(validationRecord(resolved).resolve(QuarantineResolution.purged(RESOLVED_AT, ANA)));
                    return null;
                });
                start.countDown();
                resolution.get(10, TimeUnit.SECONDS);
                List<QuarantinedRecord> listed = listing.get(10, TimeUnit.SECONDS);

                assertThat(listed).as("round %d", round).extracting(QuarantinedRecord::status)
                        .containsOnly(QuarantineStatus.PENDING);
                assertThat(listed).as("round %d", round).extracting(QuarantinedRecord::idempotencyKey)
                        .isSubsetOf(resolved, kept)
                        .filteredOn(kept::equals).hasSize(2);
                repository().resolve(validationRecord(kept).resolve(QuarantineResolution.purged(RESOLVED_AT, ANA)));
            }
        } finally {
            threads.shutdownNow();
        }
    }

    private List<QuarantinedRecord> everyPageOf(QuarantineStatus status) {
        List<QuarantinedRecord> records = new ArrayList<>();
        QuarantinePage page = repository().findByStatus(QuarantinePageRequest.first(status, 1));
        records.addAll(page.records());
        while (page.next().isPresent()) {
            page = repository().findByStatus(new QuarantinePageRequest(status, 1, page.next()));
            records.addAll(page.records());
        }
        return records;
    }

    @Test
    void findByStatus_page_returnsTheRecordsInKeyOrderAndTheKeyToGoOn() {
        repository().save(validationRecord(SECOND_KEY));
        repository().save(validationRecord(FIRST_KEY));
        repository().save(validationRecord(IdempotencyKey.of("third-key")));

        QuarantinePage first = repository().findByStatus(QuarantinePageRequest.first(QuarantineStatus.PENDING, 2));
        QuarantinePage second = repository().findByStatus(
                new QuarantinePageRequest(QuarantineStatus.PENDING, 2, first.next()));

        assertThat(first.records()).extracting(QuarantinedRecord::idempotencyKey).containsExactly(FIRST_KEY, SECOND_KEY);
        assertThat(second.records()).extracting(QuarantinedRecord::idempotencyKey).containsExactly(IdempotencyKey.of("third-key"));
        assertThat(second.next()).isEmpty();
    }

    private static QuarantinedRecord validationRecord(IdempotencyKey idempotencyKey) {
        return QuarantinedRecord.pending(envelope(idempotencyKey), QuarantineLocation.inbox(), QuarantineReason.VALIDATION_FAILED,
                List.of("price must be positive"), QUARANTINED_AT);
    }

    private static QuarantinedRecord retriesRecord(IdempotencyKey idempotencyKey) {
        return QuarantinedRecord.pending(envelope(idempotencyKey), QuarantineLocation.inbox(), QuarantineReason.RETRIES_EXHAUSTED,
                List.of(), QUARANTINED_AT);
    }

    private static Envelope envelope(IdempotencyKey idempotencyKey) {
        return new Envelope(
                idempotencyKey,
                new EventMetadata().set(EventMetadataKey.SOURCE, "boe"),
                MediaTypes.JSON,
                "{\"price\":-1}".getBytes(StandardCharsets.UTF_8),
                EnvelopeStatus.CAPTURED,
                3,
                QUARANTINED_AT,
                QUARANTINED_AT);
    }
}

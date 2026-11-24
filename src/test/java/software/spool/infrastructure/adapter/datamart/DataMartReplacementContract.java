package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a data mart writer does with the rounds of full mounts: each mount removes, when a round ends well, only what it
 * wrote before and no longer writes, wherever it is, and nothing of other mounts.
 */
abstract class DataMartReplacementContract {
    static final PartitionKey ALL = new PartitionKey("scope=all");
    static final PartitionKey SPAIN = new PartitionKey("country=ES");
    static final PartitionKey FRANCE = new PartitionKey("country=FR");
    static final MountTarget WEB = MountTarget.transformation("payments", new PartitionKey("source=web"));
    static final MountTarget APP = MountTarget.transformation("payments", new PartitionKey("source=app"));
    static final String LISTS = ".spool/";
    static final MountRoundSettings STOPPING_WITHOUT_RENEWING = new MountRoundSettings(1, Duration.ofMillis(300),
            Duration.ofHours(1), Duration.ofMillis(20));
    static final MountRoundSettings WAITING_BRIEFLY = new MountRoundSettings(1000, Duration.ofMillis(300),
            Duration.ofMillis(100), Duration.ofMillis(20));

    /** The writer of the data mart, the same one in every call, as a mounter keeps it. */
    abstract DataMartWriter writer();

    /** Another writer on the same data mart, as one in another process, whose rounds behave as the settings say. */
    abstract DataMartWriter writerWith(MountRoundSettings settings);

    /** A result with the given content, as the writer under test writes it. */
    abstract PartitionedRecord<?> result(PartitionKey partition, String content);

    /** Where the writer puts the result. */
    abstract String pathOf(MountTarget target, PartitionedRecord<?> result);

    /** The paths of every object the data mart keeps, relative to its root, the lists of the mounts included. */
    abstract List<String> allObjects();

    @Test
    void completeRound_resultThatChanged_leavesOnlyTheNewOne() {
        round(WEB, result(ALL, "total=35"));

        round(WEB, result(ALL, "total=40"));

        assertThat(results()).containsExactly(pathOf(WEB, result(ALL, "total=40")));
    }

    @Test
    void completeRound_sameResultAgain_keepsIt() {
        round(WEB, result(ALL, "total=35"));

        round(WEB, result(ALL, "total=35"));

        assertThat(results()).containsExactly(pathOf(WEB, result(ALL, "total=35")));
    }

    @Test
    void completeRound_partitionThatNoLongerGetsResults_isLeftWithoutThem() {
        round(WEB, result(SPAIN, "payment-1"), result(FRANCE, "payment-2"));

        round(WEB, result(SPAIN, "payment-1"));

        assertThat(results()).containsExactly(pathOf(WEB, result(SPAIN, "payment-1")));
    }

    @Test
    void completeRound_twoMountsThatWriteTheSamePartition_removeNothingOfTheOther() {
        round(WEB, result(SPAIN, "web-payment"));
        round(APP, result(SPAIN, "app-payment"));

        round(WEB, result(SPAIN, "web-payment"));
        round(APP, result(SPAIN, "app-payment"));

        assertThat(results()).containsExactlyInAnyOrderElementsOf(List.of(
                pathOf(WEB, result(SPAIN, "web-payment")), pathOf(APP, result(SPAIN, "app-payment"))));
    }

    @Test
    void completeRound_partOfTheScopeThatIsNoLongerMounted_isLeftWithoutItsResults() {
        MountTarget day17 = WEB.withSourceKey(new PartitionKey("source=web::day=17"));
        MountTarget day18 = WEB.withSourceKey(new PartitionKey("source=web::day=18"));
        DataMartWriter writer = writer();
        writer.beginRound(WEB);
        writer.write(day17, Stream.of(result(SPAIN, "payment-17")));
        writer.write(day18, Stream.of(result(SPAIN, "payment-18")));
        writer.completeRound(WEB);

        writer.beginRound(WEB);
        writer.write(day18, Stream.of(result(SPAIN, "payment-18")));
        writer.completeRound(WEB);

        assertThat(results()).containsExactly(pathOf(day18, result(SPAIN, "payment-18")));
    }

    @Test
    void abandonRound_removesNothing() {
        round(WEB, result(ALL, "total=35"));

        abandonedRound(WEB, result(ALL, "total=40"));

        assertThat(results()).containsExactlyInAnyOrderElementsOf(List.of(
                pathOf(WEB, result(ALL, "total=35")), pathOf(WEB, result(ALL, "total=40"))));
    }

    @Test
    void completeRound_afterOneThatWasAbandoned_removesWhatTheAbandonedOneWroteAndIsNoLongerWritten() {
        round(WEB, result(ALL, "total=35"));
        abandonedRound(WEB, result(ALL, "total=40"));

        round(WEB, result(ALL, "total=45"));

        assertThat(results()).containsExactly(pathOf(WEB, result(ALL, "total=45")));
    }

    @Test
    void beginRound_afterARoundWhoseProcessStopped_takesItsTurnAndRemovesWhatItWroteAndIsNoLongerWritten() throws Exception {
        round(WEB, result(ALL, "total=35"));
        DataMartWriter stopped = writerWith(STOPPING_WITHOUT_RENEWING);
        stopped.beginRound(WEB);
        stopped.write(WEB, Stream.of(result(ALL, "total=40")));
        DataMartWriter next = writerWith(WAITING_BRIEFLY);

        CompletableFuture.runAsync(() -> {
            next.beginRound(WEB);
            next.write(WEB, Stream.of(result(ALL, "total=45")));
            next.completeRound(WEB);
        }).get(5, TimeUnit.SECONDS);

        assertThat(results()).containsExactly(pathOf(WEB, result(ALL, "total=45")));
    }

    @Test
    void beginRound_whileAnotherRoundOfTheSameMountIsOpen_waitsUntilItEnds() throws Exception {
        DataMartWriter first = writer();
        first.beginRound(WEB);
        first.write(WEB, Stream.of(result(SPAIN, "payment-1")));
        DataMartWriter second = writerWith(WAITING_BRIEFLY);
        CompletableFuture<Void> secondRound = CompletableFuture.runAsync(() -> {
            second.beginRound(WEB);
            second.write(WEB, Stream.of(result(SPAIN, "payment-1"), result(FRANCE, "payment-2")));
            second.completeRound(WEB);
        });

        Thread.sleep(500);
        assertThat(secondRound).isNotDone();
        first.completeRound(WEB);
        secondRound.get(5, TimeUnit.SECONDS);

        assertThat(results()).containsExactlyInAnyOrderElementsOf(List.of(
                pathOf(WEB, result(SPAIN, "payment-1")), pathOf(WEB, result(FRANCE, "payment-2"))));
    }

    @Test
    void completeRound_ofARoundThatLostItsTurn_failsAndRemovesNothing() throws Exception {
        DataMartWriter stopped = writerWith(STOPPING_WITHOUT_RENEWING);
        stopped.beginRound(WEB);
        stopped.write(WEB, Stream.of(result(ALL, "total=40")));
        DataMartWriter next = writerWith(WAITING_BRIEFLY);
        CompletableFuture.runAsync(() -> {
            next.beginRound(WEB);
            next.write(WEB, Stream.of(result(ALL, "total=45")));
            next.completeRound(WEB);
        }).get(5, TimeUnit.SECONDS);

        assertThatThrownBy(() -> stopped.completeRound(WEB)).isInstanceOf(IllegalStateException.class);

        assertThat(results()).containsExactly(pathOf(WEB, result(ALL, "total=45")));
    }

    @Test
    void completeRound_resultWrittenOutsideARound_isNeverRemoved() {
        writer().write(WEB, Stream.of(result(SPAIN, "written-back-from-quarantine")));

        round(WEB, result(FRANCE, "payment-1"));
        round(WEB, result(FRANCE, "payment-2"));

        assertThat(results()).containsExactlyInAnyOrderElementsOf(List.of(
                pathOf(WEB, result(SPAIN, "written-back-from-quarantine")), pathOf(WEB, result(FRANCE, "payment-2"))));
    }

    @Test
    void completeRound_keepsTheListsOfTheMountsOutOfTheLayersOfTheDataMart() {
        round(WEB, result(ALL, "total=35"));
        abandonedRound(WEB, result(ALL, "total=40"));

        assertThat(allObjects()).anyMatch(path -> path.startsWith(LISTS));
        assertThat(results()).allMatch(path -> path.startsWith("silver/") || path.startsWith("gold/"));
    }

    void round(MountTarget target, PartitionedRecord<?>... results) {
        DataMartWriter writer = writer();
        writer.beginRound(target);
        writer.write(target, Stream.of(results));
        writer.completeRound(target);
    }

    void abandonedRound(MountTarget target, PartitionedRecord<?>... results) {
        DataMartWriter writer = writer();
        writer.beginRound(target);
        writer.write(target, Stream.of(results));
        writer.abandonRound(target);
    }

    /** The objects of the layers of the data mart, without the lists of the mounts. */
    List<String> results() {
        return allObjects().stream().filter(path -> !path.startsWith(LISTS)).sorted().toList();
    }
}

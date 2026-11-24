package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a data mart writer that notes what it writes does when a full mount ends a round: each partition it wrote keeps
 * only what was written since the last round, and nothing else changes.
 */
abstract class DataMartReplacementContract {
    static final PartitionKey ALL = new PartitionKey("scope=all");
    static final PartitionKey SPAIN = new PartitionKey("country=ES");
    static final PartitionKey FRANCE = new PartitionKey("country=FR");
    static final MountTarget TOTALS = MountTarget.aggregation("payments-total", new PartitionKey("source=payments"));
    static final MountTarget REPORTS = MountTarget.aggregation("payments-report", new PartitionKey("source=payments"));

    abstract DataMartWriter writer();

    /** The paths of the objects the data mart keeps, relative to its root. */
    abstract List<String> objects();

    @Test
    void keepOnlyWhatWasWritten_resultThatChanged_leavesOnlyTheNewOne() {
        round(TOTALS, total(ALL, 35));

        round(TOTALS, total(ALL, 40));

        assertThat(objects()).hasSize(1);
        assertThat(objectsMatching(TOTALS)).hasSize(1);
        assertThat(writtenAgain(TOTALS, total(ALL, 40))).isEqualTo(objects());
    }

    @Test
    void keepOnlyWhatWasWritten_sameResultAgain_keepsIt() {
        round(TOTALS, total(ALL, 35));
        List<String> before = objects();

        round(TOTALS, total(ALL, 35));

        assertThat(objects()).isEqualTo(before);
    }

    @Test
    void keepOnlyWhatWasWritten_partitionNotWrittenInTheRound_isLeftAsItWas() {
        round(TOTALS, total(SPAIN, 10), total(FRANCE, 20));

        round(TOTALS, total(SPAIN, 15));

        assertThat(objects()).hasSize(2);
        assertThat(objects()).anyMatch(path -> path.contains("country=FR"));
    }

    @Test
    void keepOnlyWhatWasWritten_anotherTarget_isLeftAsItWas() {
        round(REPORTS, total(ALL, 99));

        round(TOTALS, total(ALL, 35));

        assertThat(objectsMatching(REPORTS)).hasSize(1);
    }

    @Test
    void forgetWhatWasWritten_removesNothing() {
        round(TOTALS, total(ALL, 35));
        DataMartWriter writer = writer();
        writer.write(TOTALS, Stream.of(total(ALL, 40)));

        writer.forgetWhatWasWritten(TOTALS);
        round(TOTALS, total(ALL, 45));

        assertThat(objects()).hasSize(1);
        assertThat(writtenAgain(TOTALS, total(ALL, 45))).isEqualTo(objects());
    }

    @Test
    void keepOnlyWhatWasWritten_resultWithABinaryPayload_keepsItsTwoFiles() {
        round(TOTALS, new PartitionedRecord<>(ALL, BinaryResults.pdfRecord()));

        round(TOTALS, new PartitionedRecord<>(ALL, BinaryResults.pdfRecord()));

        assertThat(objects()).hasSize(2);
    }

    /** Writes the results as a round of a full mount does, and ends it well. */
    void round(MountTarget target, PartitionedRecord<?>... results) {
        DataMartWriter writer = writer();
        writer.write(target, Stream.of(results));
        writer.keepOnlyWhatWasWritten(target);
    }

    private List<String> objectsMatching(MountTarget target) {
        return objects().stream().filter(path -> path.startsWith(target.qualifiedDataMart() + "/")).toList();
    }

    private List<String> writtenAgain(MountTarget target, PartitionedRecord<?> result) {
        round(target, result);
        return objects();
    }

    static PartitionedRecord<?> total(PartitionKey partition, long total) {
        return new PartitionedRecord<>(partition, GenericRecord.of(Map.of("total", total)));
    }
}

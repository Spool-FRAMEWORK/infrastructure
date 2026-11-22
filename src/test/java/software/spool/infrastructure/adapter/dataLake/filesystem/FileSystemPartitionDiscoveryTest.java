package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemPartitionDiscoveryTest {
    @TempDir
    Path dataLake;

    @Test
    void discover_scopeThatIsTheLastFolderOfAPartition_findsThatPartition() throws IOException {
        record("silver/payments/year=2026/hour=10/source=payments/payment-1.json");
        record("silver/refunds/year=2026/hour=10/source=refunds/refund-1.json");

        assertThat(new FileSystemPartitionDiscovery(dataLake, MountMode.AGGREGATION).discover(new PartitionKey("source=payments")))
                .extracting(PartitionInfo::key)
                .containsExactly(new PartitionKey("payments::year=2026::hour=10::source=payments"));
    }

    @Test
    void discover_scopeInTheMiddleOfAPartition_findsThePartitionsBelowIt() throws IOException {
        record("bronze/year=2026/hour=10/source=payments/eventType=Payment/k1.json");
        record("bronze/year=2026/hour=10/source=refunds/eventType=Refund/k2.json");

        assertThat(new FileSystemPartitionDiscovery(dataLake, MountMode.TRANSFORMATION).discover(new PartitionKey("source=payments")))
                .extracting(PartitionInfo::key)
                .containsExactly(new PartitionKey("year=2026::hour=10::source=payments::eventType=Payment"));
    }

    private void record(String file) throws IOException {
        Path path = dataLake.resolve(file);
        Files.createDirectories(path.getParent());
        Files.writeString(path, "{}");
    }
}

package software.spool.infrastructure.adapter.dataLake.s3;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class S3PartitionDiscoveryTest {
    private static final String BUCKET = "spool-datalake-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Test
    void discover_silver_givesEachPartitionUnderTheScopeWithItsRecords() {
        s3Client.put("silver/payments/source=payments/year=2026/hour=10/a.json");
        s3Client.put("silver/payments/source=payments/year=2026/hour=10/b.json");
        s3Client.put("silver/payments/source=payments/year=2026/hour=11/c.json");
        s3Client.put("silver/refunds/source=refunds/year=2026/hour=10/d.json");

        assertThat(discovery(MountMode.AGGREGATION).discover(new PartitionKey("payments")))
                .extracting(PartitionInfo::key, PartitionInfo::estimatedRecords)
                .containsExactly(
                        tuple(new PartitionKey("payments::source=payments::year=2026::hour=10"), OptionalLong.of(2)),
                        tuple(new PartitionKey("payments::source=payments::year=2026::hour=11"), OptionalLong.of(1)));
    }

    @Test
    void discover_scopeBehindTheDateEntries_findsItWhereverItIs() {
        s3Client.put("bronze/year=2026/hour=10/source=payments/k1.json");
        s3Client.put("bronze/year=2026/hour=10/source=payments/k1.meta.json");
        s3Client.put("bronze/year=2026/hour=10/source=refunds/k2.json");

        assertThat(discovery(MountMode.TRANSFORMATION).discover(new PartitionKey("source=payments")))
                .singleElement()
                .satisfies(partition -> {
                    assertThat(partition.key()).isEqualTo(new PartitionKey("year=2026::hour=10::source=payments"));
                    assertThat(partition.estimatedRecords()).hasValue(1);
                });
    }

    @Test
    void discover_scopeWithoutObjects_findsNothing() {
        assertThat(discovery(MountMode.AGGREGATION).discover(new PartitionKey("payments"))).isEmpty();
    }

    private S3PartitionDiscovery discovery(MountMode mode) {
        return new S3PartitionDiscovery(s3Client, BUCKET, mode);
    }
}

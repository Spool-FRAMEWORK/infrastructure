package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemPartitionedReaderPartitionTest {

    private static final PayloadDeserializer<GenericRecord> JSON =
            bytes -> GenericRecord.of(PayloadDeserializerFactory.json().asMap().deserialize(bytes));

    @Test
    void read_targetThatNamesPartOfThePath_givesEachRecordItsWholePartitionFromTheLayer(@TempDir Path tmp) throws Exception {
        Path partition = tmp.resolve("bronze/year=2026/month=11/source=boe");
        Files.createDirectories(partition);
        Files.write(partition.resolve("k1.json"), "{\"id\":1}".getBytes(StandardCharsets.UTF_8));
        MountTarget target = MountTarget.transformation("boe", new PartitionKey("year=2026::month=11"));

        try (BoundedConcurrency concurrency = BoundedConcurrency.withThreads(1)) {
            List<PartitionKey> partitions = new FileSystemPartitionedReader(tmp, JSON, concurrency).read(target)
                    .map(PartitionedRecord::partitionKey).toList();

            assertThat(partitions).containsExactly(new PartitionKey("year=2026::month=11::source=boe"));
        }
    }
}

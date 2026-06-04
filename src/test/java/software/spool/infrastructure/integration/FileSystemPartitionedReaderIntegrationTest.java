package software.spool.infrastructure.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemPartitionedReader;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemPartitionedReaderIntegrationTest {

    private static final PayloadDeserializer<GenericRecord> DESERIALIZER =
            bytes -> GenericRecord.of(PayloadDeserializerFactory.json().asMap().deserialize(bytes));

    @Test
    void read_partitionPathDoesNotExist_returnsEmptyStream(@TempDir Path tmp) {
        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        List<PartitionedRecord<GenericRecord>> result = reader.read(target).toList();

        assertThat(result).isEmpty();
    }

    @Test
    void read_emptyPartitionDirectory_returnsEmptyStream(@TempDir Path tmp) throws Exception {
        Files.createDirectories(tmp.resolve("bronze/date=2026-06-04"));
        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        assertThat(reader.read(target).toList()).isEmpty();
    }

    @Test
    void read_filesInMatchingPartition_returnsRecords(@TempDir Path tmp) throws Exception {
        Path partitionDir = tmp.resolve("bronze/date=2026-06-04");
        Files.createDirectories(partitionDir);
        Files.write(partitionDir.resolve("record.json"),
                "{\"name\":\"test\",\"value\":42}".getBytes(StandardCharsets.UTF_8));

        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        List<PartitionedRecord<GenericRecord>> records = reader.read(target).toList();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).record().getString("name")).isEqualTo("test");
    }

    @Test
    void read_multipleFiles_returnsAllRecords(@TempDir Path tmp) throws Exception {
        Path partitionDir = tmp.resolve("bronze/date=2026-06-04");
        Files.createDirectories(partitionDir);
        Files.write(partitionDir.resolve("r1.json"), "{\"id\":1}".getBytes(StandardCharsets.UTF_8));
        Files.write(partitionDir.resolve("r2.json"), "{\"id\":2}".getBytes(StandardCharsets.UTF_8));
        Files.write(partitionDir.resolve("r3.json"), "{\"id\":3}".getBytes(StandardCharsets.UTF_8));

        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        assertThat(reader.read(target).toList()).hasSize(3);
    }

    @Test
    void read_aggregationMode_readsFromSilverLayer(@TempDir Path tmp) throws Exception {
        Path partitionDir = tmp.resolve("silver/month=2026-06");
        Files.createDirectories(partitionDir);
        Files.write(partitionDir.resolve("summary.json"),
                "{\"total\":99}".getBytes(StandardCharsets.UTF_8));

        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.aggregation("summary", new PartitionKey("month=2026-06"));

        List<PartitionedRecord<GenericRecord>> records = reader.read(target).toList();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).record().getLong("total")).isEqualTo(99L);
    }

    @Test
    void read_transformationMode_doesNotReadFromSilver(@TempDir Path tmp) throws Exception {
        Path silverDir = tmp.resolve("silver/date=2026-06-04");
        Files.createDirectories(silverDir);
        Files.write(silverDir.resolve("record.json"), "{\"id\":1}".getBytes(StandardCharsets.UTF_8));

        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        assertThat(reader.read(target).toList()).isEmpty();
    }

    @Test
    void read_returnedStream_isCloseableWithoutError(@TempDir Path tmp) throws Exception {
        Path partitionDir = tmp.resolve("bronze/date=2026-06-04");
        Files.createDirectories(partitionDir);
        Files.write(partitionDir.resolve("record.json"), "{\"id\":1}".getBytes(StandardCharsets.UTF_8));

        FileSystemPartitionedReader reader = new FileSystemPartitionedReader(tmp, DESERIALIZER);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        try (Stream<PartitionedRecord<GenericRecord>> stream = reader.read(target)) {
            assertThat(stream.toList()).hasSize(1);
        }
    }
}

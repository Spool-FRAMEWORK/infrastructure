package software.spool.infrastructure.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.datamart.FileSystemDataMartWriter;
import software.spool.infrastructure.adapter.datamart.RawFileSystemDataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemDataMartWriterIntegrationTest {

    @Test
    void write_transformation_createsFileInSilverDirectory(@TempDir Path tmp) {
        FileSystemDataMartWriter writer = new FileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));
        PartitionedRecord<Map<String, Object>> record = new PartitionedRecord<>(
                new PartitionKey("date=2026-06-04"), Map.of("id", "abc", "amount", 100));

        writer.write(target, Stream.of(record));

        Path dir = tmp.resolve("silver/orders/date=2026-06-04");
        assertThat(dir).isDirectory();
        assertThat(dir.toFile().listFiles()).hasSize(1);
    }

    @Test
    void write_aggregation_createsFileInGoldDirectory(@TempDir Path tmp) {
        FileSystemDataMartWriter writer = new FileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.aggregation("summary", new PartitionKey("month=2026-06"));
        PartitionedRecord<Map<String, Object>> record = new PartitionedRecord<>(
                new PartitionKey("month=2026-06"), Map.of("total", 42));

        writer.write(target, Stream.of(record));

        Path dir = tmp.resolve("gold/summary/month=2026-06");
        assertThat(dir).isDirectory();
        assertThat(dir.toFile().listFiles()).hasSize(1);
    }

    @Test
    void write_emptyStream_createsNoOutputDirectory(@TempDir Path tmp) {
        FileSystemDataMartWriter writer = new FileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

        writer.write(target, Stream.empty());

        assertThat(tmp.resolve("silver/orders")).doesNotExist();
    }

    @Test
    void write_multipleRecords_createsOneFilePerRecord(@TempDir Path tmp) {
        FileSystemDataMartWriter writer = new FileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));
        PartitionedRecord<Map<String, Object>> r1 = new PartitionedRecord<>(
                new PartitionKey("date=2026-06-04"), Map.of("id", "a"));
        PartitionedRecord<Map<String, Object>> r2 = new PartitionedRecord<>(
                new PartitionKey("date=2026-06-04"), Map.of("id", "b"));

        writer.write(target, Stream.of(r1, r2));

        Path dir = tmp.resolve("silver/orders/date=2026-06-04");
        assertThat(dir.toFile().listFiles()).hasSize(2);
    }

    @Test
    void write_fileExtensionIsJson_whenNoExtensionResolverSet(@TempDir Path tmp) {
        FileSystemDataMartWriter writer = new FileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));
        PartitionedRecord<Map<String, Object>> record = new PartitionedRecord<>(
                new PartitionKey("date=2026-06-04"), Map.of("id", "abc"));

        writer.write(target, Stream.of(record));

        Path dir = tmp.resolve("silver/orders/date=2026-06-04");
        String[] filenames = dir.toFile().list();
        assertThat(filenames).hasSize(1);
        assertThat(filenames[0]).endsWith(".json");
    }

    @Test
    void raw_write_writesExactBytesToFile(@TempDir Path tmp) throws Exception {
        byte[] content = "%PDF-1.4 binary content".getBytes(StandardCharsets.UTF_8);
        RawFileSystemDataMartWriter writer = new RawFileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.transformation("pdfs", new PartitionKey("date=2026-06-04"), "pdf");
        PartitionedRecord<byte[]> record = new PartitionedRecord<>(new PartitionKey("date=2026-06-04"), content);

        writer.write(target, Stream.of(record));

        Path dir = tmp.resolve("silver/pdfs/date=2026-06-04");
        assertThat(dir).isDirectory();
        java.io.File[] files = dir.toFile().listFiles();
        assertThat(files).hasSize(1);
        assertThat(files[0].getName()).endsWith(".pdf");
        assertThat(Files.readAllBytes(files[0].toPath())).isEqualTo(content);
    }

    @Test
    void raw_write_emptyStream_createsNoOutputDirectory(@TempDir Path tmp) {
        RawFileSystemDataMartWriter writer = new RawFileSystemDataMartWriter(tmp);
        MountTarget target = MountTarget.transformation("pdfs", new PartitionKey("date=2026-06-04"), "pdf");

        writer.write(target, Stream.empty());

        assertThat(tmp.resolve("silver/pdfs")).doesNotExist();
    }
}

package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemDataMartWriterRecordsTest {

    private static final PartitionKey PARTITION = new PartitionKey("source=payments");
    private static final MountTarget TARGET = MountTarget.transformation("payments", PARTITION);

    private record DailyTotal(String day, long total) {
    }

    @TempDir
    Path dataMart;

    @Test
    void write_recordWithoutTransformationAndJavaRecord_writesTheFieldsOfBoth() throws IOException {
        try (BoundedConcurrency concurrency = BoundedConcurrency.withThreads(1)) {
            new FileSystemDataMartWriter(dataMart, concurrency).write(TARGET, Stream.of(
                    new PartitionedRecord<>(PARTITION, GenericRecord.of(Map.of("id", "payment-1", "amount", 10))),
                    new PartitionedRecord<>(PARTITION, new DailyTotal("2026-11-17", 35))));
        }

        assertThat(contentsUnder(dataMart)).containsExactlyInAnyOrder(
                Map.of("id", "payment-1", "amount", 10),
                Map.of("day", "2026-11-17", "total", 35));
    }

    private static List<Map<String, Object>> contentsUnder(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile)
                    .map(file -> PayloadDeserializerFactory.json().asMap().deserialize(read(file)))
                    .toList();
        }
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

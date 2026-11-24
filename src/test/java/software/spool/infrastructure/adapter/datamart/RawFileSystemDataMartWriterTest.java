package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class RawFileSystemDataMartWriterTest {
    private static final PartitionKey PARTITION = new PartitionKey("source=reports");
    private static final MountTarget TARGET = MountTarget.transformation("reports", PARTITION, "txt");

    @TempDir
    Path dataMart;

    @Test
    void write_sameResultTwice_keepsASingleFile() throws IOException {
        write("the same report".getBytes(StandardCharsets.UTF_8));
        write("the same report".getBytes(StandardCharsets.UTF_8));

        assertThat(filesUnder(dataMart)).hasSize(1);
    }

    @Test
    void write_twoDifferentResults_keepsBoth() throws IOException {
        write("first report".getBytes(StandardCharsets.UTF_8));
        write("second report".getBytes(StandardCharsets.UTF_8));

        assertThat(filesUnder(dataMart)).hasSize(2);
    }

    private void write(byte[] content) {
        try (BoundedConcurrency concurrency = BoundedConcurrency.withThreads(1)) {
            new RawFileSystemDataMartWriter(dataMart, concurrency).write(TARGET, Stream.of(new PartitionedRecord<>(PARTITION, content)));
        }
    }

    private static List<Path> filesUnder(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).toList();
        }
    }
}

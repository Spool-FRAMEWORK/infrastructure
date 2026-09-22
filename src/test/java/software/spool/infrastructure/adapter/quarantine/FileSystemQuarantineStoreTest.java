package software.spool.infrastructure.adapter.quarantine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.ingester.api.port.QuarantinedRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemQuarantineStoreTest {

    @TempDir
    Path directory;

    private FileSystemQuarantineStore store;

    @BeforeEach
    void createStore() {
        store = new FileSystemQuarantineStore(directory.toString());
    }

    @Test
    void send_aRecord_writesItAsAFileThatCanBeReadBack() throws IOException {
        QuarantinedRecord record = new QuarantinedRecord(
                "{\"id\":1}".getBytes(),
                List.of("id must be positive"),
                Instant.parse("2026-09-21T10:00:00Z"));

        store.send(record);

        List<Path> files = listFiles();
        assertThat(files).hasSize(1);
        QuarantinedRecord written = read(files.get(0));
        assertThat(written.original()).isEqualTo(record.original());
        assertThat(written.violations()).containsExactly("id must be positive");
        assertThat(written.quarantinedAt()).isEqualTo(record.quarantinedAt());
    }

    @Test
    void send_twoRecords_writesTwoDistinctFiles() {
        store.send(new QuarantinedRecord("{}".getBytes(), List.of("a"), Instant.now()));
        store.send(new QuarantinedRecord("{}".getBytes(), List.of("b"), Instant.now()));

        assertThat(listFiles()).hasSize(2);
    }

    @Test
    void send_createsTheDirectoryIfItDoesNotExist() {
        FileSystemQuarantineStore intoMissingDirectory = new FileSystemQuarantineStore(directory.resolve("nested").toString());

        intoMissingDirectory.send(new QuarantinedRecord("{}".getBytes(), List.of("a"), Instant.now()));

        assertThat(Files.exists(directory.resolve("nested"))).isTrue();
    }

    private List<Path> listFiles() {
        try (Stream<Path> files = Files.list(directory)) {
            return files.toList();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static QuarantinedRecord read(Path file) throws IOException {
        return PayloadDeserializerFactory.json().as(QuarantinedRecord.class).deserialize(Files.readAllBytes(file));
    }
}

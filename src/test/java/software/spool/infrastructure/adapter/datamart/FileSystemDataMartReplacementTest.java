package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

class FileSystemDataMartReplacementTest extends DataMartReplacementContract {
    @TempDir
    Path dataMart;

    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private FileSystemDataMartWriter writer;

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Override
    DataMartWriter writer() {
        if (writer == null) writer = new FileSystemDataMartWriter(dataMart, concurrency);
        return writer;
    }

    @Override
    List<String> objects() {
        try (Stream<Path> walk = Files.walk(dataMart)) {
            return walk.filter(Files::isRegularFile)
                    .map(file -> dataMart.relativize(file).toString().replace(java.io.File.separatorChar, '/'))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

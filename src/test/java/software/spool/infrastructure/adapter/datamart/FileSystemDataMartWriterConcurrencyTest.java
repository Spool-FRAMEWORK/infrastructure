package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemDataMartWriterConcurrencyTest {

    private static final MountTarget TARGET = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));

    private final List<BoundedConcurrency> opened = new ArrayList<>();

    private BoundedConcurrency threads(int threads) {
        BoundedConcurrency concurrency = BoundedConcurrency.withThreads(threads);
        opened.add(concurrency);
        return concurrency;
    }

    @AfterEach
    void closeThePools() {
        opened.forEach(BoundedConcurrency::close);
    }

    private static Stream<PartitionedRecord<?>> records(int count) {
        return IntStream.range(0, count).mapToObj(i ->
                new PartitionedRecord<Map<String, Object>>(new PartitionKey("date=2026-06-04"), Map.of("id", i)));
    }

    private static Map<String, String> contentsOf(Path root) throws Exception {
        Map<String, String> contents = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path file : walk.filter(Files::isRegularFile).toList())
                contents.put(root.relativize(file).toString().replace('\\', '/'), Files.readString(file));
        }
        return contents;
    }

    @Test
    void manyThreadsWriteTheSameFilesAsOneThread(@TempDir Path tmp) throws Exception {
        Path single = tmp.resolve("single");
        Path many = tmp.resolve("many");

        new FileSystemDataMartWriter(single, threads(1)).write(TARGET, records(60));
        new FileSystemDataMartWriter(many, threads(8)).write(TARGET, records(60));

        assertThat(contentsOf(many)).hasSize(60).isEqualTo(contentsOf(single));
    }

    @Test
    void recordsAreWrittenOnThreadsOfItsOwnPool(@TempDir Path tmp) {
        ConcurrentLinkedQueue<String> threadNames = new ConcurrentLinkedQueue<>();
        RecordSerializer<Object> recording = record -> {
            threadNames.add(Thread.currentThread().getName());
            return RecordSerializerFactory.record().serialize(record);
        };

        new FileSystemDataMartWriter(tmp, recording, threads(4)).write(TARGET, records(20));

        assertThat(threadNames).hasSize(20).allMatch(name -> name.startsWith("spool-io-"));
    }

    @Test
    void recordsAreReallyWrittenAtTheSameTime(@TempDir Path tmp) {
        CountDownLatch allWriting = new CountDownLatch(4);
        AtomicInteger released = new AtomicInteger();
        RecordSerializer<Object> waiting = record -> {
            allWriting.countDown();
            try {
                if (allWriting.await(5, TimeUnit.SECONDS)) released.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return RecordSerializerFactory.record().serialize(record);
        };

        new FileSystemDataMartWriter(tmp, waiting, threads(4)).write(TARGET, records(4));

        assertThat(released.get()).isEqualTo(4);
    }

    @Test
    void theStreamIsConsumedByTheCallerAndNotByTheWorkers(@TempDir Path tmp) {
        Thread caller = Thread.currentThread();
        ConcurrentLinkedQueue<Thread> producers = new ConcurrentLinkedQueue<>();
        Stream<PartitionedRecord<?>> aggregated = records(20).peek(record -> producers.add(Thread.currentThread()));

        new FileSystemDataMartWriter(tmp, threads(4)).write(TARGET, aggregated);

        assertThat(producers).hasSize(20).allMatch(thread -> thread == caller);
    }

    @Test
    void aFailureWhileWritingIsThrown(@TempDir Path tmp) throws Exception {
        Path notADirectory = Files.createFile(tmp.resolve("file"));

        assertThatThrownBy(() -> new FileSystemDataMartWriter(notADirectory, threads(3)).write(TARGET, records(6)))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void anEmptyStreamCreatesNothing(@TempDir Path tmp) throws Exception {
        new FileSystemDataMartWriter(tmp, threads(3)).write(TARGET, Stream.empty());

        assertThat(contentsOf(tmp)).isEmpty();
    }

    @Test
    void theRawWriterWritesTheSameFilesAsOneThread(@TempDir Path tmp) throws Exception {
        Path single = tmp.resolve("single");
        Path many = tmp.resolve("many");
        List<PartitionedRecord<?>> bytes = IntStream.range(0, 40).<PartitionedRecord<?>>mapToObj(i ->
                new PartitionedRecord<byte[]>(new PartitionKey("date=2026-06-04"), ("{\"id\":" + i + "}").getBytes())).toList();

        new RawFileSystemDataMartWriter(single, threads(1)).write(TARGET, bytes.stream());
        new RawFileSystemDataMartWriter(many, threads(8)).write(TARGET, bytes.stream());

        assertThat(contentsOf(many)).hasSize(40).isEqualTo(contentsOf(single));
    }
}

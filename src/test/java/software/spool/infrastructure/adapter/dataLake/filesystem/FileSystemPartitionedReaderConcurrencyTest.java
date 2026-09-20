package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.AfterEach;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemPartitionedReaderConcurrencyTest {

    private static final MountTarget TARGET = MountTarget.transformation("orders", new PartitionKey("date=2026-06-04"));
    private static final PayloadDeserializer<GenericRecord> JSON =
            bytes -> GenericRecord.of(PayloadDeserializerFactory.json().asMap().deserialize(bytes));

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

    private static void writeFiles(Path tmp, int count) throws Exception {
        Path partition = tmp.resolve("bronze/date=2026-06-04");
        Files.createDirectories(partition);
        for (int i = 0; i < count; i++)
            Files.write(partition.resolve(String.format("r%03d.json", i)),
                    ("{\"id\":" + i + "}").getBytes(StandardCharsets.UTF_8));
    }

    private static List<Object> ids(FileSystemPartitionedReader reader) {
        try (var records = reader.read(TARGET)) {
            return records.map(PartitionedRecord::record).map(record -> record.get("id")).toList();
        }
    }

    @Test
    void manyThreadsReadTheSameRecordsInTheSameOrderAsOneThread(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 60);

        List<Object> withOne = ids(new FileSystemPartitionedReader(tmp, JSON, threads(1)));
        List<Object> withMany = ids(new FileSystemPartitionedReader(tmp, JSON, threads(8)));

        assertThat(withMany).hasSize(60).isEqualTo(withOne);
    }

    @Test
    void theRecordsComeInTheOrderOfTheFiles(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 30);

        List<Object> ids = ids(new FileSystemPartitionedReader(tmp, JSON, threads(6)));

        assertThat(ids).containsExactlyElementsOf(IntStream.range(0, 30).boxed().map(i -> (Object) i).toList());
    }

    @Test
    void theStreamHandedToTheAggregatorIsSequential(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 5);

        try (var records = new FileSystemPartitionedReader(tmp, JSON, threads(4)).read(TARGET)) {
            assertThat(records.isParallel()).isFalse();
        }
    }

    @Test
    void filesAreReadOnThreadsOfItsOwnPoolAndNotTheCommonPool(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 20);
        ConcurrentLinkedQueue<String> threadNames = new ConcurrentLinkedQueue<>();
        PayloadDeserializer<GenericRecord> recording = bytes -> {
            threadNames.add(Thread.currentThread().getName());
            return JSON.deserialize(bytes);
        };

        ids(new FileSystemPartitionedReader(tmp, recording, threads(4)));

        assertThat(threadNames).hasSize(20).allMatch(name -> name.startsWith("spool-io-"));
    }

    @Test
    void filesAreReallyReadAtTheSameTime(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 4);
        CountDownLatch allReading = new CountDownLatch(4);
        AtomicInteger released = new AtomicInteger();
        PayloadDeserializer<GenericRecord> waiting = bytes -> {
            allReading.countDown();
            try {
                if (allReading.await(5, TimeUnit.SECONDS)) released.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return JSON.deserialize(bytes);
        };

        ids(new FileSystemPartitionedReader(tmp, waiting, threads(4)));

        assertThat(released.get()).isEqualTo(4);
    }

    @Test
    void aFailureWhileReadingAFileFailsTheRead(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 6);
        PayloadDeserializer<GenericRecord> failing = bytes -> {
            GenericRecord record = JSON.deserialize(bytes);
            if (Integer.valueOf(3).equals(record.get("id"))) throw new IllegalStateException("cannot read 3");
            return record;
        };

        assertThatThrownBy(() -> ids(new FileSystemPartitionedReader(tmp, failing, threads(3))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cannot read 3");
    }

    @Test
    void theDefaultReaderKeepsWorking(@TempDir Path tmp) throws Exception {
        writeFiles(tmp, 10);

        assertThat(ids(new FileSystemPartitionedReader(tmp, JSON))).hasSize(10);
    }
}

package software.spool.infrastructure.adapter.datamart;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Writes the records of a partition to the file system, several files at a time.
 *
 * <p>The records are taken from the stream by the calling thread, one at a time, so the aggregator that produces
 * them stays sequential. Only the writing of each file runs on the threads of a {@link BoundedConcurrency}.</p>
 *
 * <p>It keeps the list of what each mount wrote, as {@link MountRounds} says, so a round of a full mount that ends well
 * removes what the mount no longer writes.</p>
 */
public class FileSystemDataMartWriter implements DataMartWriter {
    private final DataMartStore store;
    private final RecordSerializer<Object> serializer;
    private final BoundedConcurrency concurrency;
    private final MountRounds rounds;

    /**
     * Creates a writer that owns a pool with one thread per available processor.
     *
     * @param basePath the root of the data mart
     */
    public FileSystemDataMartWriter(Path basePath) {
        this(basePath, BoundedConcurrency.withThreads(Runtime.getRuntime().availableProcessors()));
    }

    /**
     * Creates a writer that writes on the given concurrency, which the caller closes.
     *
     * @param basePath    the root of the data mart
     * @param concurrency the pool and window the files are written with
     */
    public FileSystemDataMartWriter(Path basePath, BoundedConcurrency concurrency) {
        this(basePath, RecordSerializerFactory.record(), concurrency);
    }

    /**
     * Creates a writer with its own way of serializing the records.
     *
     * @param basePath    the root of the data mart
     * @param serializer  turns a record into the bytes of a file
     * @param concurrency the pool and window the files are written with
     */
    public FileSystemDataMartWriter(Path basePath, RecordSerializer<Object> serializer, BoundedConcurrency concurrency) {
        this(basePath, serializer, concurrency, MountRoundSettings.DEFAULT);
    }

    FileSystemDataMartWriter(Path basePath, RecordSerializer<Object> serializer, BoundedConcurrency concurrency,
                             MountRoundSettings rounds) {
        this.store = new FileSystemDataMartStore(basePath);
        this.serializer = serializer;
        this.concurrency = concurrency;
        this.rounds = new MountRounds(store, rounds);
    }

    public FileSystemDataMartWriter(String basePath) {
        this(Path.of(basePath));
    }

    public FileSystemDataMartWriter(String basePath, BoundedConcurrency concurrency) {
        this(Path.of(basePath), concurrency);
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        concurrency.forEach(result, partitioned -> DataMartObject.of(target, partitioned, serializer).forEach(object -> {
            store.write(object.path(), object.content());
            rounds.note(target, object.path());
        }));
    }

    @Override
    public void beginRound(MountTarget target) {
        rounds.begin(target);
    }

    @Override
    public void completeRound(MountTarget target) {
        rounds.complete(target);
    }

    @Override
    public void abandonRound(MountTarget target) {
        rounds.abandon(target);
    }
}

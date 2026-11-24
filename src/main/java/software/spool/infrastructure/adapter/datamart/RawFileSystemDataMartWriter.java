package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Writes the records of a partition, which are already bytes, to the file system, several files at a time.
 *
 * <p>The records are taken from the stream by the calling thread, one at a time, so the aggregator that produces
 * them stays sequential. Only the writing of each file runs on the threads of a {@link BoundedConcurrency}.</p>
 *
 * <p>It keeps the list of what each mount wrote, as {@link MountRounds} says, so a round of a full mount that ends well
 * removes what the mount no longer writes.</p>
 */
public class RawFileSystemDataMartWriter implements DataMartWriter {
    private static final String SEPARATOR = "/";
    private static final String EXTENSION_SEPARATOR = ".";

    private final DataMartStore store;
    private final BoundedConcurrency concurrency;
    private final MountRounds rounds;

    /**
     * Creates a writer that owns a pool with one thread per available processor.
     *
     * @param basePath the root of the data mart
     */
    public RawFileSystemDataMartWriter(Path basePath) {
        this(basePath, BoundedConcurrency.withThreads(Runtime.getRuntime().availableProcessors()));
    }

    /**
     * Creates a writer that writes on the given concurrency, which the caller closes.
     *
     * @param basePath    the root of the data mart
     * @param concurrency the pool and window the files are written with
     */
    public RawFileSystemDataMartWriter(Path basePath, BoundedConcurrency concurrency) {
        this(basePath, concurrency, MountRoundSettings.DEFAULT);
    }

    RawFileSystemDataMartWriter(Path basePath, BoundedConcurrency concurrency, MountRoundSettings rounds) {
        this.store = new FileSystemDataMartStore(basePath);
        this.concurrency = concurrency;
        this.rounds = new MountRounds(store, rounds);
    }

    public RawFileSystemDataMartWriter(String basePath) {
        this(Path.of(basePath));
    }

    public RawFileSystemDataMartWriter(String basePath, BoundedConcurrency concurrency) {
        this(Path.of(basePath), concurrency);
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        concurrency.forEach(result, partitioned -> {
            byte[] content = (byte[]) partitioned.record();
            String path = target.qualifiedDataMart() + SEPARATOR
                    + String.join(SEPARATOR, KeyEntries.split(partitioned.partitionKey().value())) + SEPARATOR
                    + IdempotencyKey.of(target.dataMart(), content).value() + EXTENSION_SEPARATOR + target.resolveExtension(content);
            store.write(path, content);
            rounds.note(target, path);
        });
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

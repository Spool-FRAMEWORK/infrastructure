package software.spool.infrastructure.adapter.datamart;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.stream.Stream;

/**
 * Writes the results of a mount to a bucket, the objects of each laid out as {@link DataMartObject} says, several at a
 * time on the threads of a {@link BoundedConcurrency}. The results are taken from the stream by the calling thread, so
 * the aggregator that produces them stays sequential.
 *
 * <p>It keeps the list of what each mount wrote, as {@link MountRounds} says, so a round of a full mount that ends well
 * removes what the mount no longer writes.</p>
 */
public class S3DataMartWriter implements DataMartWriter {
    private final DataMartStore store;
    private final RecordSerializer<Object> serializer;
    private final BoundedConcurrency concurrency;
    private final MountRounds rounds;

    public S3DataMartWriter(S3Client s3Client, String bucket, BoundedConcurrency concurrency) {
        this(s3Client, bucket, RecordSerializerFactory.record(), concurrency);
    }

    public S3DataMartWriter(S3Client s3Client, String bucket, RecordSerializer<Object> serializer, BoundedConcurrency concurrency) {
        this(s3Client, bucket, serializer, concurrency, MountRoundSettings.DEFAULT);
    }

    S3DataMartWriter(S3Client s3Client, String bucket, RecordSerializer<Object> serializer, BoundedConcurrency concurrency,
                     MountRoundSettings rounds) {
        this.store = new S3DataMartStore(s3Client, bucket);
        this.serializer = serializer;
        this.concurrency = concurrency;
        this.rounds = new MountRounds(store, rounds);
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

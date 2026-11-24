package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class S3DataMartReplacementTest extends DataMartReplacementContract {
    private static final String BUCKET = "spool-datamart-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private final S3DataMartWriter writer = new S3DataMartWriter(s3Client, BUCKET, RecordSerializerFactory.record(), concurrency);

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Test
    void completeRound_resultWithABinaryPayload_keepsItsTwoObjects() {
        round(WEB, new PartitionedRecord<>(ALL, BinaryResults.pdfRecord()));

        round(WEB, new PartitionedRecord<>(ALL, BinaryResults.pdfRecord()));

        assertThat(results()).hasSize(2);
    }

    @Override
    DataMartWriter writer() {
        return writer;
    }

    @Override
    DataMartWriter writerWith(MountRoundSettings settings) {
        return new S3DataMartWriter(s3Client, BUCKET, RecordSerializerFactory.record(), concurrency, settings);
    }

    @Override
    PartitionedRecord<?> result(PartitionKey partition, String content) {
        return new PartitionedRecord<>(partition, GenericRecord.of(Map.of("value", content)));
    }

    @Override
    String pathOf(MountTarget target, PartitionedRecord<?> result) {
        return DataMartObject.of(target, result, RecordSerializerFactory.record()).getFirst().path();
    }

    @Override
    List<String> allObjects() {
        return s3Client.keys().stream().sorted().toList();
    }
}

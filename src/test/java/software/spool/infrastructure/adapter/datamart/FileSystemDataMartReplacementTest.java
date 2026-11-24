package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemDataMartReplacementTest extends DataMartReplacementContract {
    @TempDir
    Path dataMart;

    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private FileSystemDataMartWriter writer;

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Test
    void completeRound_resultWithABinaryPayload_keepsItsTwoFiles() {
        round(WEB, new PartitionedRecord<>(ALL, BinaryResults.pdfRecord()));

        round(WEB, new PartitionedRecord<>(ALL, BinaryResults.pdfRecord()));

        assertThat(results()).hasSize(2);
    }

    @Override
    DataMartWriter writer() {
        if (writer == null) writer = new FileSystemDataMartWriter(dataMart, RecordSerializerFactory.record(), concurrency);
        return writer;
    }

    @Override
    DataMartWriter writerWith(MountRoundSettings settings) {
        return new FileSystemDataMartWriter(dataMart, RecordSerializerFactory.record(), concurrency, settings);
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
        return new FileSystemDataMartStore(dataMart).objectsUnder("").stream().sorted().toList();
    }
}

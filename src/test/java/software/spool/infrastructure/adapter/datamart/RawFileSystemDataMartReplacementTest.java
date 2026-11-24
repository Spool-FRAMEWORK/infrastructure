package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

class RawFileSystemDataMartReplacementTest extends DataMartReplacementContract {
    @TempDir
    Path dataMart;

    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private RawFileSystemDataMartWriter writer;

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Override
    DataMartWriter writer() {
        if (writer == null) writer = new RawFileSystemDataMartWriter(dataMart, concurrency);
        return writer;
    }

    @Override
    DataMartWriter writerWith(MountRoundSettings settings) {
        return new RawFileSystemDataMartWriter(dataMart, concurrency, settings);
    }

    @Override
    PartitionedRecord<?> result(PartitionKey partition, String content) {
        return new PartitionedRecord<>(partition, content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    String pathOf(MountTarget target, PartitionedRecord<?> result) {
        byte[] content = (byte[]) result.record();
        return target.qualifiedDataMart() + "/" + String.join("/", KeyEntries.split(result.partitionKey().value())) + "/"
                + IdempotencyKey.of(target.dataMart(), content).value() + "." + target.resolveExtension(content);
    }

    @Override
    List<String> allObjects() {
        return new FileSystemDataMartStore(dataMart).objectsUnder("").stream().sorted().toList();
    }
}

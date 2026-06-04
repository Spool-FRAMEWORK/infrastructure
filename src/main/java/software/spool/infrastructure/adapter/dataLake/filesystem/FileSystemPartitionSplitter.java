package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.port.logging.Logger;
import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.api.port.scaling.PartitionSlice;
import software.spool.mounter.api.port.scaling.PartitionSplitter;
import software.spool.mounter.api.port.scaling.SplitHint;

import java.util.ArrayList;
import java.util.List;

public class FileSystemPartitionSplitter implements PartitionSplitter {

    private static final Logger log = LoggerFactory.getLogger("FileSystemPartitionSplitter");

    @Override
    public List<PartitionSlice> split(PartitionInfo partition, SplitHint hint) {
        long total = partition.estimatedRecords().orElse(0L);
        long batch = hint.targetRecordsPerSlice();
        long count = (total + batch - 1) / batch;

        log.info("Splitting partition {} — {} files into {} slice(s) of {} each",
                partition.key().value(), total, count, batch);

        List<PartitionSlice> slices = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            slices.add(new PartitionSlice(partition.key(), i * batch, batch));
        }
        return slices;
    }
}

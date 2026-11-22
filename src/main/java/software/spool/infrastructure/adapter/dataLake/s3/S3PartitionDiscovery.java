package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Lists the partitions of a bucket laid out by {@link S3DataLakeLayout} that hold objects under a scope, with how many
 * records each one holds: of bronze for a {@link MountMode#TRANSFORMATION} mount, and of silver for a
 * {@link MountMode#AGGREGATION} one. The header of an envelope kept next to its payload is not counted as a record.
 */
public class S3PartitionDiscovery implements PartitionDiscovery {
    private final S3PartitionObjects objects;
    private final MountMode mode;

    public S3PartitionDiscovery(S3Client s3Client, String bucket, MountMode mode) {
        this.objects = new S3PartitionObjects(s3Client, bucket);
        this.mode = mode;
    }

    @Override
    public List<PartitionInfo> discover(PartitionKey scope) {
        String layer = S3DataLakeLayout.layerReadBy(mode);
        Map<PartitionKey, Long> recordsByPartition = new LinkedHashMap<>();
        objects.keysOf(layer, scope).stream()
                .filter(key -> !S3DataLakeLayout.isHeaderKey(key))
                .forEach(key -> recordsByPartition.merge(S3DataLakeLayout.partitionOf(layer, key), 1L, Long::sum));
        return recordsByPartition.entrySet().stream()
                .map(partition -> new PartitionInfo(partition.getKey(), OptionalLong.of(partition.getValue()), OptionalLong.empty()))
                .toList();
    }
}

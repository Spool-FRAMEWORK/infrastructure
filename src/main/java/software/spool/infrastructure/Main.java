package software.spool.infrastructure;

import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<PartitionedRecord<GenericRecord>> list = PluginResolver.resolve(PartitionedReaderProvider.class, PluginConfiguration.builder().with("path", "D:/spool/datalake").build())
                .read(new PartitionKey("year=2026::hour=17"));
        System.out.println(list.getFirst().partitionKey());
    }
}

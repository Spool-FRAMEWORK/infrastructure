package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.ingester.api.port.DataLakeWriter;

@SpoolPlugin(DataLakeWriterProvider.class)
public class S3DataLakeWriterProvider implements DataLakeWriterProvider {
    @Override
    public String name() {
        return "S3";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("region") &&
                configuration.has("bucket") &&
                configuration.has("endpoint");
    }

    @Override
    public DataLakeWriter create(PluginConfiguration configuration) {
        return new S3DataLakeWriter(
                S3ClientFactory.create(configuration),
                configuration.require("bucket"));
    }
}

package software.spool.infrastructure.adapter.datamart;

import software.spool.infrastructure.adapter.concurrency.ConcurrencyConfiguration;
import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.mounter.api.port.DataMartWriter;

@SpoolPlugin(DataMartWriterProvider.class)
public class S3DataMartWriterProvider implements DataMartWriterProvider {
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
    public DataMartWriter create(PluginConfiguration configuration) {
        return new S3DataMartWriter(
                S3ClientFactory.create(configuration),
                configuration.require("bucket"),
                ConcurrencyConfiguration.from(configuration));
    }
}
